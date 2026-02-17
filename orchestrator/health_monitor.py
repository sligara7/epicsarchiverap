"""Health Monitor module — tracks the health state of primary and secondary clusters."""

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from config import ClusterConfig, OrchestratorConfig

logger = logging.getLogger(__name__)


class ClusterState(str, enum.Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"
    RECOVERING = "RECOVERING"


@dataclass
class ClusterHealth:
    """Current health snapshot for a single cluster."""

    name: str
    state: ClusterState = ClusterState.HEALTHY
    total_appliances: int = 0
    active_appliances: int = 0
    total_pvs: int = 0
    pv_status: dict[str, int] = field(default_factory=dict)
    last_check: float = 0.0
    last_healthy: float = 0.0
    outage_start: float | None = None
    outage_end: float | None = None
    error: str = ""


class HealthMonitor:
    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self.primary_health = ClusterHealth(name=config.primary.name)
        self.secondary_health = ClusterHealth(name=config.secondary.name)
        self._outage_history: list[dict] = []

    def get_health(self, cluster_name: str) -> ClusterHealth:
        if cluster_name == self.config.primary.name:
            return self.primary_health
        return self.secondary_health

    @property
    def outage_history(self) -> list[dict]:
        return list(self._outage_history)

    async def check_cluster(
        self, session: aiohttp.ClientSession, cluster: ClusterConfig
    ) -> ClusterHealth:
        """Check health of a single cluster using getClusterHealthSummary."""
        health = self.get_health(cluster.name)
        now = time.time()
        health.last_check = now
        previous_state = health.state

        try:
            url = f"{cluster.mgmt_url}/getClusterHealthSummary"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    health.error = f"HTTP {resp.status}"
                    health = self._transition_state(health, ClusterState.DOWN, now)
                    return health

                data = await resp.json()

            health.total_appliances = data.get("totalAppliances", 0)
            health.active_appliances = data.get("activeAppliances", 0)
            health.total_pvs = data.get("totalPVs", 0)
            health.pv_status = data.get("pvsByStatus", {})
            health.error = ""

            new_state = self._evaluate_state(health, previous_state)
            health = self._transition_state(health, new_state, now)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            health.error = str(e)
            logger.warning("Cluster %s unreachable: %s", cluster.name, e)
            health = self._transition_state(health, ClusterState.DOWN, now)
        except Exception:
            logger.exception("Unexpected error checking cluster %s", cluster.name)
            health.error = "Unexpected error"
            health = self._transition_state(health, ClusterState.DOWN, now)

        return health

    def _evaluate_state(
        self, health: ClusterHealth, previous_state: ClusterState
    ) -> ClusterState:
        """Evaluate the cluster state based on health data."""
        if health.total_appliances == 0:
            return ClusterState.DOWN

        active_ratio = health.active_appliances / health.total_appliances

        # Check appliance-level health
        if active_ratio < 0.5:
            return ClusterState.DOWN

        # Check PV-level health
        total_status_pvs = sum(health.pv_status.values())
        appliance_down_pvs = health.pv_status.get("Appliance Down", 0)
        being_archived_pvs = health.pv_status.get("Being archived", 0)

        if total_status_pvs > 0:
            down_ratio = appliance_down_pvs / total_status_pvs
            archived_ratio = being_archived_pvs / total_status_pvs if total_status_pvs > 0 else 0

            if down_ratio > 0.5:
                return ClusterState.DOWN
            if down_ratio > 0.05 or active_ratio < 1.0:
                return ClusterState.DEGRADED
        elif active_ratio < 1.0:
            return ClusterState.DEGRADED

        # If previously DOWN and now looks ok, transition through RECOVERING
        if previous_state == ClusterState.DOWN:
            return ClusterState.RECOVERING

        if previous_state == ClusterState.RECOVERING:
            # Stay recovering until PVs settle
            if total_status_pvs > 0:
                archived_ratio = being_archived_pvs / total_status_pvs
                if archived_ratio >= 0.95:
                    return ClusterState.HEALTHY
                return ClusterState.RECOVERING
            return ClusterState.HEALTHY

        return ClusterState.HEALTHY

    def _transition_state(
        self, health: ClusterHealth, new_state: ClusterState, now: float
    ) -> ClusterHealth:
        """Handle state transitions and record outage boundaries."""
        old_state = health.state
        health.state = new_state

        if new_state == ClusterState.HEALTHY:
            health.last_healthy = now

        # Record outage start
        if old_state != ClusterState.DOWN and new_state == ClusterState.DOWN:
            health.outage_start = now
            health.outage_end = None
            logger.warning("Cluster %s is DOWN (outage started)", health.name)

        # Record outage end
        if old_state == ClusterState.DOWN and new_state != ClusterState.DOWN:
            health.outage_end = now
            if health.outage_start is not None:
                self._outage_history.append(
                    {
                        "cluster": health.name,
                        "start": health.outage_start,
                        "end": now,
                    }
                )
            logger.info("Cluster %s recovering from DOWN (outage ended)", health.name)

        if old_state != new_state:
            logger.info(
                "Cluster %s state: %s -> %s", health.name, old_state.value, new_state.value
            )

        return health

    async def run_check(self) -> tuple[ClusterHealth, ClusterHealth]:
        """Run health checks on both clusters concurrently."""
        async with aiohttp.ClientSession() as session:
            primary, secondary = await asyncio.gather(
                self.check_cluster(session, self.config.primary),
                self.check_cluster(session, self.config.secondary),
            )

        self.primary_health = primary
        self.secondary_health = secondary
        return primary, secondary
