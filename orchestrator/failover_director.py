"""Failover Director module — manages client failover between primary and secondary clusters."""

import abc
import logging
import time

import aiohttp

from config import OrchestratorConfig
from health_monitor import ClusterState, HealthMonitor

logger = logging.getLogger(__name__)


class FailoverStrategy(abc.ABC):
    """Abstract base for failover strategies."""

    @abc.abstractmethod
    async def switch_to_primary(self) -> bool:
        """Point clients at the primary cluster."""

    @abc.abstractmethod
    async def switch_to_secondary(self) -> bool:
        """Point clients at the secondary cluster."""

    @abc.abstractmethod
    async def get_active_target(self) -> str:
        """Return which cluster clients currently point to."""


class DNSFailoverStrategy(FailoverStrategy):
    """DNS-based failover — updates a CNAME record.

    This is a skeleton implementation. Real deployments would integrate
    with Route53, PowerDNS, or another DNS provider's API.
    """

    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self._active = "primary"

    async def switch_to_primary(self) -> bool:
        dns_record = self.config.failover.dns_record
        target = self.config.primary.data_retrieval_url
        logger.info("DNS failover: updating %s -> %s (primary)", dns_record, target)
        # Integrate with DNS provider API here:
        # e.g. Route53 change_resource_record_sets, PowerDNS patch, etc.
        self._active = "primary"
        return True

    async def switch_to_secondary(self) -> bool:
        dns_record = self.config.failover.dns_record
        target = self.config.secondary.data_retrieval_url
        logger.info("DNS failover: updating %s -> %s (secondary)", dns_record, target)
        # Integrate with DNS provider API here
        self._active = "secondary"
        return True

    async def get_active_target(self) -> str:
        return self._active


class ProxyFailoverStrategy(FailoverStrategy):
    """Reverse-proxy-based failover — updates nginx/HAProxy upstream config via API."""

    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self._active = "primary"

    async def switch_to_primary(self) -> bool:
        logger.info("Proxy failover: switching upstream to primary")
        # Integrate with proxy management API here
        self._active = "primary"
        return True

    async def switch_to_secondary(self) -> bool:
        logger.info("Proxy failover: switching upstream to secondary")
        # Integrate with proxy management API here
        self._active = "secondary"
        return True

    async def get_active_target(self) -> str:
        return self._active


class FailoverDirector:
    """Manages failover decisions based on health monitoring."""

    def __init__(self, config: OrchestratorConfig, health_monitor: HealthMonitor):
        self.config = config
        self.health = health_monitor
        self._active_cluster = "primary"
        self._primary_down_since: float | None = None
        self._failover_executed = False
        self._manual_override: str | None = None

        if config.failover.strategy == "proxy":
            self.strategy: FailoverStrategy = ProxyFailoverStrategy(config)
        else:
            self.strategy = DNSFailoverStrategy(config)

    @property
    def active_cluster(self) -> str:
        return self._active_cluster

    async def evaluate(self) -> str | None:
        """Evaluate health state and determine if failover action is needed.

        Returns the name of the action taken, or None.
        """
        primary = self.health.primary_health
        secondary = self.health.secondary_health
        now = time.time()

        # If manual override is set, respect it
        if self._manual_override:
            return None

        # Track how long primary has been down
        if primary.state == ClusterState.DOWN:
            if self._primary_down_since is None:
                self._primary_down_since = now
        else:
            self._primary_down_since = None

        # Failover: primary down -> switch to secondary
        if (
            self._active_cluster == "primary"
            and primary.state == ClusterState.DOWN
            and secondary.state in (ClusterState.HEALTHY, ClusterState.DEGRADED)
        ):
            if self._primary_down_since is not None:
                down_duration = now - self._primary_down_since
                if self.config.failover.auto_failover and down_duration >= self.config.failover.auto_failover_delay:
                    return await self._do_failover_to_secondary()

        # Failback: primary recovered -> switch back
        if (
            self._active_cluster == "secondary"
            and primary.state == ClusterState.HEALTHY
            and self._failover_executed
        ):
            if self.config.failover.auto_failback:
                return await self._do_failback_to_primary()

        return None

    async def _do_failover_to_secondary(self) -> str:
        logger.warning("AUTO-FAILOVER: switching clients to secondary cluster")
        ok = await self.strategy.switch_to_secondary()
        if ok:
            self._active_cluster = "secondary"
            self._failover_executed = True
            return "failover_to_secondary"
        logger.error("Failover to secondary FAILED")
        return "failover_failed"

    async def _do_failback_to_primary(self) -> str:
        logger.info("AUTO-FAILBACK: switching clients back to primary cluster")
        ok = await self.strategy.switch_to_primary()
        if ok:
            self._active_cluster = "primary"
            self._failover_executed = False
            self._primary_down_since = None
            return "failback_to_primary"
        logger.error("Failback to primary FAILED")
        return "failback_failed"

    async def manual_switch(self, target: str) -> bool:
        """Manually switch to the specified cluster."""
        if target == "secondary":
            ok = await self.strategy.switch_to_secondary()
            if ok:
                self._active_cluster = "secondary"
                self._manual_override = "secondary"
                logger.info("Manual failover to secondary")
                return True
        elif target == "primary":
            ok = await self.strategy.switch_to_primary()
            if ok:
                self._active_cluster = "primary"
                self._manual_override = None
                self._failover_executed = False
                logger.info("Manual failback to primary")
                return True
        return False

    async def clear_manual_override(self):
        """Clear any manual override, allowing automatic failover again."""
        self._manual_override = None
        logger.info("Manual override cleared")
