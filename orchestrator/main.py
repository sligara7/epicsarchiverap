"""Main entry point for the archiver orchestrator service."""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path

from aiohttp import web

from api import build_app
from config import OrchestratorConfig, load_config
from failover_director import FailoverDirector
from health_monitor import HealthMonitor
from pv_sync import PVSync
from reconciliation import Reconciliation

logger = logging.getLogger("orchestrator")


class Orchestrator:
    """Main orchestrator that coordinates all modules."""

    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self.health_monitor = HealthMonitor(config)
        self.pv_sync = PVSync(config)
        self.failover_director = FailoverDirector(config, self.health_monitor)
        self.reconciliation = Reconciliation(config, self.health_monitor)
        self._shutdown_event = asyncio.Event()
        self._state_file = Path(config.state_file)

    async def _health_loop(self):
        """Periodically check cluster health."""
        while not self._shutdown_event.is_set():
            try:
                primary, secondary = await self.health_monitor.run_check()
                logger.debug(
                    "Health: primary=%s, secondary=%s",
                    primary.state.value,
                    secondary.state.value,
                )

                # Evaluate failover
                action = await self.failover_director.evaluate()
                if action:
                    logger.warning("Failover action: %s", action)
                    await self._send_alert(
                        f"Failover action: {action} "
                        f"(primary={primary.state.value}, secondary={secondary.state.value})"
                    )

                # Auto-reconcile if configured
                if (
                    self.config.reconciliation.auto_reconcile
                    and self.failover_director.active_cluster == "primary"
                    and primary.state.value == "HEALTHY"
                    and self.health_monitor.outage_history
                    and not self.reconciliation.progress.running
                ):
                    logger.info("Auto-triggering reconciliation")
                    asyncio.create_task(self.reconciliation.run_reconciliation())

                # Warn if unreconciled outage is approaching secondary MTS
                # retention limit (blackhole will discard the gap data)
                await self._check_retention_deadline()

                self._save_state()

            except Exception:
                logger.exception("Error in health check loop")

            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.health_check_interval,
                )
                break
            except asyncio.TimeoutError:
                pass

    async def _sync_loop(self):
        """Periodically sync PV configurations."""
        # Wait a bit on startup so health checks run first
        await asyncio.sleep(5)

        while not self._shutdown_event.is_set():
            try:
                result = await self.pv_sync.run_sync()
                if result.added or result.removed:
                    logger.info(
                        "PV sync: +%d -%d", len(result.added), len(result.removed)
                    )
            except Exception:
                logger.exception("Error in PV sync loop")

            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.sync_interval,
                )
                break
            except asyncio.TimeoutError:
                pass

    async def _check_retention_deadline(self):
        """Alert if an unreconciled outage is approaching the secondary's MTS retention limit."""
        retention_days = self.config.reconciliation.secondary_retention_days
        if retention_days <= 0:
            return

        retention_secs = retention_days * 86400
        warn_threshold = retention_secs * 0.75  # warn at 75% of retention
        now = time.time()

        for outage in self.health_monitor.outage_history:
            if outage["cluster"] != self.config.primary.name:
                continue
            age = now - outage["start"]
            if age > warn_threshold and not self.reconciliation.progress.running:
                days_left = max(0, (retention_secs - age) / 86400)
                await self._send_alert(
                    f"URGENT: Unreconciled primary outage from "
                    f"{time.strftime('%Y-%m-%d %H:%M', time.gmtime(outage['start']))} "
                    f"has ~{days_left:.1f} days before secondary MTS data is "
                    f"discarded by blackhole ETL. Reconcile NOW."
                )

    async def _send_alert(self, message: str):
        """Send an alert via webhook if configured."""
        if not self.config.alerting.webhook_url:
            return

        import aiohttp

        try:
            async with aiohttp.ClientSession() as session:
                payload = {"text": f"[Archiver Orchestrator] {message}"}
                async with session.post(
                    self.config.alerting.webhook_url, json=payload
                ) as resp:
                    if resp.status != 200:
                        logger.warning("Alert webhook returned %d", resp.status)
        except Exception:
            logger.exception("Failed to send alert")

    def _save_state(self):
        """Persist orchestrator state to disk."""
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            state = {
                "active_cluster": self.failover_director.active_cluster,
                "primary_state": self.health_monitor.primary_health.state.value,
                "secondary_state": self.health_monitor.secondary_health.state.value,
                "outage_history": self.health_monitor.outage_history,
            }
            with open(self._state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception:
            logger.debug("Could not save state file", exc_info=True)

    def _load_state(self):
        """Load orchestrator state from disk."""
        if not self._state_file.exists():
            return
        try:
            with open(self._state_file) as f:
                state = json.load(f)
            logger.info("Loaded state from %s", self._state_file)
            if state.get("active_cluster") == "secondary":
                logger.warning("Previous state had secondary active — operator should verify")
        except Exception:
            logger.debug("Could not load state file", exc_info=True)

    async def run(self):
        """Start all orchestrator loops and the API server."""
        self._load_state()

        app = build_app(
            self.config,
            self.health_monitor,
            self.pv_sync,
            self.failover_director,
            self.reconciliation,
        )

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.config.api_port)
        await site.start()
        logger.info("API server listening on port %d", self.config.api_port)

        # Start background loops
        health_task = asyncio.create_task(self._health_loop())
        sync_task = asyncio.create_task(self._sync_loop())

        # Wait for shutdown
        await self._shutdown_event.wait()

        health_task.cancel()
        sync_task.cancel()
        await runner.cleanup()
        logger.info("Orchestrator shut down")

    def shutdown(self):
        self._shutdown_event.set()


def main():
    parser = argparse.ArgumentParser(description="Archiver Orchestrator Service")
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    config = load_config(args.config)
    orchestrator = Orchestrator(config)

    loop = asyncio.new_event_loop()

    def _handle_signal():
        logger.info("Received shutdown signal")
        orchestrator.shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        loop.run_until_complete(orchestrator.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
