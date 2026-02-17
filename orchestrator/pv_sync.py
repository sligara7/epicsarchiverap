"""PV Sync module — synchronizes PV archival configuration from primary to secondary cluster."""

import asyncio
import logging
from dataclasses import dataclass, field
from urllib.parse import quote

import aiohttp

from config import ClusterConfig, OrchestratorConfig

logger = logging.getLogger(__name__)


@dataclass
class PVConfig:
    """Slimmed-down PV archival configuration."""

    pv_name: str
    sampling_period: float = 1.0
    sampling_method: str = "MONITOR"
    policy_name: str = "Default"
    archive_fields: list[str] = field(default_factory=list)
    use_pv_access: bool = False
    paused: bool = False


@dataclass
class SyncResult:
    """Result of a PV sync operation."""

    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    failed_adds: list[str] = field(default_factory=list)
    failed_removes: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


class PVSync:
    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self.last_result: SyncResult | None = None
        self._running = False

    async def get_primary_config(self, session: aiohttp.ClientSession) -> dict[str, PVConfig]:
        """Fetch the archival configuration from the primary cluster."""
        url = f"{self.config.primary.mgmt_url}/exportArchivalConfig"
        logger.info("Fetching primary archival config from %s", url)
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        configs = {}
        for item in data:
            pv = PVConfig(
                pv_name=item["pvName"],
                sampling_period=item.get("samplingPeriod", 1.0),
                sampling_method=item.get("samplingMethod", "MONITOR"),
                policy_name=item.get("policyName", "Default"),
                archive_fields=item.get("archiveFields", []),
                use_pv_access=item.get("usePVAccess", False),
                paused=item.get("paused", False),
            )
            configs[pv.pv_name] = pv
        logger.info("Primary has %d PVs configured", len(configs))
        return configs

    async def get_secondary_pvs(self, session: aiohttp.ClientSession) -> set[str]:
        """Fetch the list of all PVs currently archived on the secondary."""
        url = f"{self.config.secondary.mgmt_url}/getAllPVs"
        params = {"limit": "-1"}
        logger.info("Fetching secondary PV list from %s", url)
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        pvs = set(data) if isinstance(data, list) else set()
        logger.info("Secondary has %d PVs", len(pvs))
        return pvs

    async def archive_pv_on_secondary(
        self, session: aiohttp.ClientSession, pv_config: PVConfig
    ) -> bool:
        """Submit a single PV for archiving on the secondary cluster."""
        url = f"{self.config.secondary.mgmt_url}/archivePV"
        params = {
            "pv": pv_config.pv_name,
            "samplingperiod": str(pv_config.sampling_period),
            "samplingmethod": pv_config.sampling_method,
        }
        if pv_config.use_pv_access:
            params["usePVAccess"] = "true"

        try:
            async with session.get(url, params=params) as resp:
                body = await resp.json(content_type=None)
                status_val = body.get("status", "") if isinstance(body, dict) else ""
                if resp.status == 200 or "Already" in str(status_val):
                    return True
                logger.warning(
                    "archivePV failed for %s: %s", pv_config.pv_name, body
                )
                return False
        except Exception:
            logger.exception("Error archiving PV %s on secondary", pv_config.pv_name)
            return False

    async def delete_pv_on_secondary(
        self, session: aiohttp.ClientSession, pv_name: str
    ) -> bool:
        """Pause and then delete a PV from the secondary cluster."""
        mgmt = self.config.secondary.mgmt_url
        try:
            # Pause first
            pause_url = f"{mgmt}/pauseArchivingPV"
            async with session.get(pause_url, params={"pv": pv_name}) as resp:
                await resp.read()

            # Then delete
            delete_url = f"{mgmt}/deletePV"
            async with session.get(
                delete_url, params={"pv": pv_name, "deleteData": "false"}
            ) as resp:
                body = await resp.json(content_type=None)
                if resp.status == 200:
                    return True
                logger.warning("deletePV failed for %s: %s", pv_name, body)
                return False
        except Exception:
            logger.exception("Error deleting PV %s from secondary", pv_name)
            return False

    async def wait_for_batch_archived(
        self,
        session: aiohttp.ClientSession,
        pv_names: list[str],
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> set[str]:
        """Poll getPVStatus until PVs reach 'Being archived' or timeout."""
        url = f"{self.config.secondary.mgmt_url}/getPVStatus"
        remaining = set(pv_names)
        archived = set()
        elapsed = 0.0

        while remaining and elapsed < timeout:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            pv_param = ",".join(remaining)
            try:
                async with session.get(url, params={"pv": pv_param}) as resp:
                    if resp.status != 200:
                        continue
                    statuses = await resp.json()
                    if not isinstance(statuses, list):
                        continue
                    for item in statuses:
                        if isinstance(item, dict) and item.get("status") == "Being archived":
                            pv = item.get("pvName", "")
                            if pv in remaining:
                                remaining.discard(pv)
                                archived.add(pv)
            except Exception:
                logger.debug("Error polling PV status, will retry", exc_info=True)

        if remaining:
            logger.warning(
                "%d PVs did not reach 'Being archived' within %ds: %s",
                len(remaining),
                timeout,
                list(remaining)[:5],
            )
        return archived

    async def run_sync(self) -> SyncResult:
        """Execute a full PV sync cycle."""
        if self._running:
            logger.warning("Sync already in progress, skipping")
            return self.last_result or SyncResult()

        self._running = True
        result = SyncResult()
        try:
            async with aiohttp.ClientSession() as session:
                primary_config = await self.get_primary_config(session)
                secondary_pvs = await self.get_secondary_pvs(session)

                primary_pv_names = set(primary_config.keys())
                to_add = primary_pv_names - secondary_pvs
                to_remove = secondary_pvs - primary_pv_names

                logger.info(
                    "Sync diff: %d to add, %d to remove", len(to_add), len(to_remove)
                )

                # Batch additions
                batch_size = self.config.archive_batch_size
                to_add_list = sorted(to_add)
                for i in range(0, len(to_add_list), batch_size):
                    batch = to_add_list[i : i + batch_size]
                    logger.info(
                        "Archiving batch %d-%d of %d",
                        i,
                        i + len(batch),
                        len(to_add_list),
                    )

                    tasks = []
                    for pv_name in batch:
                        pv_config = primary_config[pv_name]
                        tasks.append(
                            self.archive_pv_on_secondary(session, pv_config)
                        )

                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    submitted = []
                    for pv_name, ok in zip(batch, results):
                        if ok is True:
                            submitted.append(pv_name)
                            result.added.append(pv_name)
                        else:
                            result.failed_adds.append(pv_name)

                    # Wait for batch to be archived before next batch
                    if submitted:
                        await self.wait_for_batch_archived(session, submitted)

                    if i + batch_size < len(to_add_list):
                        await asyncio.sleep(self.config.batch_delay)

                # Handle removals
                for pv_name in sorted(to_remove):
                    ok = await self.delete_pv_on_secondary(session, pv_name)
                    if ok:
                        result.removed.append(pv_name)
                    else:
                        result.failed_removes.append(pv_name)

        except Exception:
            logger.exception("PV sync failed")
        finally:
            self._running = False
            self.last_result = result

        logger.info(
            "Sync complete: %d added, %d removed, %d add failures, %d remove failures",
            len(result.added),
            len(result.removed),
            len(result.failed_adds),
            len(result.failed_removes),
        )
        return result
