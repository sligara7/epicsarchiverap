"""Reconciliation module — backfills primary with data captured by secondary during outages."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from urllib.parse import quote

import aiohttp

from config import OrchestratorConfig
from health_monitor import HealthMonitor

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationProgress:
    """Tracks the progress of a reconciliation operation."""

    total_pvs: int = 0
    completed_pvs: int = 0
    failed_pvs: list[str] = field(default_factory=list)
    skipped_pvs: list[str] = field(default_factory=list)
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    outage_start: float = 0.0
    outage_end: float = 0.0


class Reconciliation:
    def __init__(self, config: OrchestratorConfig, health_monitor: HealthMonitor):
        self.config = config
        self.health = health_monitor
        self.progress = ReconciliationProgress()
        self._running = False

    async def merge_pv(
        self,
        session: aiohttp.ClientSession,
        pv_name: str,
        secondary_retrieval_url: str,
        target_store: str,
    ) -> bool:
        """Merge data for a single PV from secondary into primary.

        Steps:
        1. Pause archiving on primary
        2. mergeInData from secondary (the archiver internally appends /data/getData.raw)
        3. Resume archiving on primary
        """
        mgmt_url = self.config.primary.mgmt_url

        try:
            # Step 1: Pause archiving on primary
            pause_url = f"{mgmt_url}/pauseArchivingPV"
            async with session.get(pause_url, params={"pv": pv_name}) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("Failed to pause %s: %s", pv_name, body)
                    return False
                await resp.read()

            # Step 2: Merge data from secondary into the target store on primary.
            # The 'other' param is the secondary's data_retrieval_url — the archiver
            # appends /data/getData.raw internally (see MergeInDataFromExternalStore.java:129).
            merge_url = f"{mgmt_url}/mergeInData"
            params = {
                "pv": pv_name,
                "other": secondary_retrieval_url,
                "storage": target_store,
            }
            async with session.get(
                merge_url, params=params, timeout=aiohttp.ClientTimeout(total=300)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("mergeInData failed for %s: %s", pv_name, body)
                    # Still resume even if merge fails
                else:
                    logger.info("mergeInData succeeded for %s", pv_name)

            # Step 3: Resume archiving on primary
            resume_url = f"{mgmt_url}/resumeArchivingPV"
            async with session.get(resume_url, params={"pv": pv_name}) as resp:
                await resp.read()

            return True

        except Exception:
            logger.exception("Error during reconciliation of %s", pv_name)
            # Try to resume archiving even on error
            try:
                resume_url = f"{mgmt_url}/resumeArchivingPV"
                async with session.get(resume_url, params={"pv": pv_name}) as resp:
                    await resp.read()
            except Exception:
                logger.exception("Failed to resume %s after error", pv_name)
            return False

    async def get_primary_pvs(self, session: aiohttp.ClientSession) -> list[str]:
        """Get the list of all PVs on the primary cluster."""
        url = f"{self.config.primary.mgmt_url}/getAllPVs"
        async with session.get(url, params={"limit": "-1"}) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return data if isinstance(data, list) else []

    async def run_reconciliation(
        self, outage_start: float | None = None, outage_end: float | None = None
    ) -> ReconciliationProgress:
        """Run reconciliation for all PVs after a primary outage.

        If outage_start/outage_end not provided, uses the latest outage from health monitor.
        """
        if self._running:
            logger.warning("Reconciliation already in progress")
            return self.progress

        self._running = True
        self.progress = ReconciliationProgress(running=True, start_time=time.time())

        # Determine outage window
        if outage_start is None or outage_end is None:
            history = self.health.outage_history
            primary_outages = [h for h in history if h["cluster"] == self.config.primary.name]
            if primary_outages:
                latest = primary_outages[-1]
                outage_start = latest["start"]
                outage_end = latest["end"]
            else:
                logger.warning("No outage history found, nothing to reconcile")
                self.progress.running = False
                self._running = False
                return self.progress

        self.progress.outage_start = outage_start
        self.progress.outage_end = outage_end
        logger.info(
            "Starting reconciliation for outage window %.0f - %.0f",
            outage_start,
            outage_end,
        )

        try:
            async with aiohttp.ClientSession() as session:
                pv_list = await self.get_primary_pvs(session)
                self.progress.total_pvs = len(pv_list)
                logger.info("Reconciling %d PVs", len(pv_list))

                target_store = self.config.reconciliation.merge_target_store
                secondary_retrieval = self.config.secondary.data_retrieval_url

                # Process PVs serially (one at a time per the plan constraints)
                for pv_name in pv_list:
                    ok = await self.merge_pv(
                        session, pv_name, secondary_retrieval, target_store
                    )
                    if ok:
                        self.progress.completed_pvs += 1
                    else:
                        self.progress.failed_pvs.append(pv_name)

        except Exception:
            logger.exception("Reconciliation failed")
        finally:
            self.progress.running = False
            self.progress.end_time = time.time()
            self._running = False

        logger.info(
            "Reconciliation complete: %d/%d succeeded, %d failed",
            self.progress.completed_pvs,
            self.progress.total_pvs,
            len(self.progress.failed_pvs),
        )
        return self.progress
