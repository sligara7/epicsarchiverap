"""Tests for the failover director module."""

import asyncio
import time

import pytest

from config import ClusterConfig, FailoverConfig, OrchestratorConfig
from failover_director import FailoverDirector
from health_monitor import ClusterHealth, ClusterState, HealthMonitor


def _make_config(**failover_kwargs):
    return OrchestratorConfig(
        primary=ClusterConfig("primary", "http://p:17665/mgmt/bpl", "http://p:17668/retrieval"),
        secondary=ClusterConfig("secondary", "http://s:17665/mgmt/bpl", "http://s:17668/retrieval"),
        failover=FailoverConfig(
            strategy="dns",
            auto_failover=failover_kwargs.get("auto_failover", False),
            auto_failover_delay=failover_kwargs.get("auto_failover_delay", 0),
            auto_failback=failover_kwargs.get("auto_failback", False),
        ),
    )


class TestFailoverEvaluation:
    @pytest.mark.asyncio
    async def test_no_action_when_healthy(self):
        config = _make_config(auto_failover=True, auto_failover_delay=0)
        monitor = HealthMonitor(config)
        monitor.primary_health = ClusterHealth(name="primary", state=ClusterState.HEALTHY)
        monitor.secondary_health = ClusterHealth(name="secondary", state=ClusterState.HEALTHY)

        director = FailoverDirector(config, monitor)
        action = await director.evaluate()
        assert action is None
        assert director.active_cluster == "primary"

    @pytest.mark.asyncio
    async def test_auto_failover_when_primary_down(self):
        config = _make_config(auto_failover=True, auto_failover_delay=0)
        monitor = HealthMonitor(config)
        monitor.primary_health = ClusterHealth(name="primary", state=ClusterState.DOWN)
        monitor.secondary_health = ClusterHealth(name="secondary", state=ClusterState.HEALTHY)

        director = FailoverDirector(config, monitor)
        # First call sets _primary_down_since and triggers failover (delay=0)
        action = await director.evaluate()
        assert action == "failover_to_secondary"
        assert director.active_cluster == "secondary"

    @pytest.mark.asyncio
    async def test_no_auto_failover_when_disabled(self):
        config = _make_config(auto_failover=False)
        monitor = HealthMonitor(config)
        monitor.primary_health = ClusterHealth(name="primary", state=ClusterState.DOWN)
        monitor.secondary_health = ClusterHealth(name="secondary", state=ClusterState.HEALTHY)

        director = FailoverDirector(config, monitor)
        await director.evaluate()
        action = await director.evaluate()
        assert action is None
        assert director.active_cluster == "primary"

    @pytest.mark.asyncio
    async def test_auto_failback(self):
        config = _make_config(auto_failover=True, auto_failover_delay=0, auto_failback=True)
        monitor = HealthMonitor(config)

        director = FailoverDirector(config, monitor)

        # Failover first
        monitor.primary_health = ClusterHealth(name="primary", state=ClusterState.DOWN)
        monitor.secondary_health = ClusterHealth(name="secondary", state=ClusterState.HEALTHY)
        await director.evaluate()
        await director.evaluate()
        assert director.active_cluster == "secondary"

        # Primary recovers
        monitor.primary_health = ClusterHealth(name="primary", state=ClusterState.HEALTHY)
        action = await director.evaluate()
        assert action == "failback_to_primary"
        assert director.active_cluster == "primary"

    @pytest.mark.asyncio
    async def test_manual_switch(self):
        config = _make_config()
        monitor = HealthMonitor(config)
        director = FailoverDirector(config, monitor)

        ok = await director.manual_switch("secondary")
        assert ok
        assert director.active_cluster == "secondary"

        ok = await director.manual_switch("primary")
        assert ok
        assert director.active_cluster == "primary"

    @pytest.mark.asyncio
    async def test_manual_override_blocks_auto(self):
        config = _make_config(auto_failover=True, auto_failover_delay=0)
        monitor = HealthMonitor(config)
        director = FailoverDirector(config, monitor)

        # Manual switch to secondary
        await director.manual_switch("secondary")

        # Even with primary recovering, auto-failback won't trigger
        monitor.primary_health = ClusterHealth(name="primary", state=ClusterState.HEALTHY)
        monitor.secondary_health = ClusterHealth(name="secondary", state=ClusterState.HEALTHY)
        action = await director.evaluate()
        assert action is None  # Manual override blocks auto
