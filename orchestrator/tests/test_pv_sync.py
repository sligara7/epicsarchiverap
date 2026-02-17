"""Tests for the PV sync module."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from config import ClusterConfig, OrchestratorConfig
from pv_sync import PVConfig, PVSync


class TestPVConfig:
    def test_defaults(self):
        pv = PVConfig(pv_name="TEST:PV:1")
        assert pv.sampling_period == 1.0
        assert pv.sampling_method == "MONITOR"
        assert pv.paused is False


class TestPVSyncDiff:
    """Test the sync diff logic with mock HTTP servers."""

    @pytest.fixture
    def config(self):
        return OrchestratorConfig(
            archive_batch_size=10,
            batch_delay=0,
            primary=ClusterConfig("primary", "http://primary:17665/mgmt/bpl", "http://primary:17668/retrieval"),
            secondary=ClusterConfig("secondary", "http://secondary:17665/mgmt/bpl", "http://secondary:17668/retrieval"),
        )

    def test_pv_config_creation(self):
        item = {
            "pvName": "TEST:PV:1",
            "samplingPeriod": 0.5,
            "samplingMethod": "SCAN",
            "policyName": "Fast",
            "archiveFields": ["HIHI", "LOLO"],
            "usePVAccess": True,
            "paused": False,
        }
        pv = PVConfig(
            pv_name=item["pvName"],
            sampling_period=item.get("samplingPeriod", 1.0),
            sampling_method=item.get("samplingMethod", "MONITOR"),
            policy_name=item.get("policyName", "Default"),
            archive_fields=item.get("archiveFields", []),
            use_pv_access=item.get("usePVAccess", False),
            paused=item.get("paused", False),
        )
        assert pv.pv_name == "TEST:PV:1"
        assert pv.sampling_period == 0.5
        assert pv.sampling_method == "SCAN"
        assert pv.use_pv_access is True
        assert pv.archive_fields == ["HIHI", "LOLO"]
