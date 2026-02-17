"""Tests for the health monitor module."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config import ClusterConfig, OrchestratorConfig
from health_monitor import ClusterHealth, ClusterState, HealthMonitor


def _make_config():
    return OrchestratorConfig(
        primary=ClusterConfig("primary", "http://primary:17665/mgmt/bpl", "http://primary:17668/retrieval"),
        secondary=ClusterConfig("secondary", "http://secondary:17665/mgmt/bpl", "http://secondary:17668/retrieval"),
    )


class TestClusterStateEvaluation:
    def test_all_healthy(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(
            name="primary",
            total_appliances=3,
            active_appliances=3,
            pv_status={"Being archived": 1000, "Paused": 10},
        )
        state = monitor._evaluate_state(health, ClusterState.HEALTHY)
        assert state == ClusterState.HEALTHY

    def test_degraded_when_appliance_down(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(
            name="primary",
            total_appliances=3,
            active_appliances=2,
            pv_status={"Being archived": 900, "Paused": 10},
        )
        state = monitor._evaluate_state(health, ClusterState.HEALTHY)
        assert state == ClusterState.DEGRADED

    def test_down_when_majority_unreachable(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(
            name="primary",
            total_appliances=3,
            active_appliances=1,
            pv_status={"Being archived": 100},
        )
        state = monitor._evaluate_state(health, ClusterState.HEALTHY)
        assert state == ClusterState.DOWN

    def test_down_when_no_appliances(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(name="primary", total_appliances=0, active_appliances=0)
        state = monitor._evaluate_state(health, ClusterState.HEALTHY)
        assert state == ClusterState.DOWN

    def test_recovering_from_down(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(
            name="primary",
            total_appliances=3,
            active_appliances=3,
            pv_status={"Being archived": 900, "Paused": 100},
        )
        state = monitor._evaluate_state(health, ClusterState.DOWN)
        assert state == ClusterState.RECOVERING

    def test_recovering_to_healthy(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(
            name="primary",
            total_appliances=3,
            active_appliances=3,
            pv_status={"Being archived": 990, "Paused": 10},
        )
        state = monitor._evaluate_state(health, ClusterState.RECOVERING)
        assert state == ClusterState.HEALTHY

    def test_degraded_when_high_appliance_down_pvs(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(
            name="primary",
            total_appliances=3,
            active_appliances=3,
            pv_status={"Being archived": 800, "Appliance Down": 100, "Paused": 100},
        )
        state = monitor._evaluate_state(health, ClusterState.HEALTHY)
        assert state == ClusterState.DEGRADED


class TestStateTransitions:
    def test_outage_recording(self):
        monitor = HealthMonitor(_make_config())
        health = ClusterHealth(name="primary", state=ClusterState.HEALTHY)

        # Transition to DOWN
        health = monitor._transition_state(health, ClusterState.DOWN, 1000.0)
        assert health.outage_start == 1000.0
        assert health.outage_end is None

        # Transition to RECOVERING
        health = monitor._transition_state(health, ClusterState.RECOVERING, 1060.0)
        assert health.outage_end == 1060.0
        assert len(monitor.outage_history) == 1
        assert monitor.outage_history[0]["start"] == 1000.0
        assert monitor.outage_history[0]["end"] == 1060.0
