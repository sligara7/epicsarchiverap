"""Tests for the configuration module."""

import tempfile
from pathlib import Path

import pytest
import yaml

from config import load_config


def _write_config(tmp_path: Path, data: dict) -> Path:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(data))
    return config_file


def test_load_minimal_config(tmp_path):
    data = {
        "primary_cluster": {
            "name": "primary",
            "mgmt_url": "http://primary:17665/mgmt/bpl",
            "data_retrieval_url": "http://primary:17668/retrieval",
        },
        "secondary_cluster": {
            "name": "secondary",
            "mgmt_url": "http://secondary:17665/mgmt/bpl",
            "data_retrieval_url": "http://secondary:17668/retrieval",
        },
    }
    config = load_config(_write_config(tmp_path, data))
    assert config.primary.mgmt_url == "http://primary:17665/mgmt/bpl"
    assert config.secondary.name == "secondary"
    assert config.sync_interval == 300
    assert config.health_check_interval == 30


def test_load_full_config(tmp_path):
    data = {
        "orchestrator": {
            "sync_interval": 60,
            "health_check_interval": 10,
            "archive_batch_size": 50,
            "api_port": 8080,
            "state_file": "/tmp/test-state.json",
        },
        "primary_cluster": {
            "name": "prod-primary",
            "mgmt_url": "http://p:17665/mgmt/bpl",
            "data_retrieval_url": "http://p:17668/retrieval",
        },
        "secondary_cluster": {
            "name": "prod-secondary",
            "mgmt_url": "http://s:17665/mgmt/bpl",
            "data_retrieval_url": "http://s:17668/retrieval",
        },
        "failover": {
            "strategy": "proxy",
            "auto_failover": True,
            "auto_failover_delay": 60,
        },
        "reconciliation": {
            "auto_reconcile": True,
            "max_concurrent_merges": 2,
            "merge_target_store": "MTS",
        },
    }
    config = load_config(_write_config(tmp_path, data))
    assert config.sync_interval == 60
    assert config.archive_batch_size == 50
    assert config.failover.strategy == "proxy"
    assert config.failover.auto_failover is True
    assert config.reconciliation.merge_target_store == "MTS"


def test_missing_config_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")


def test_env_override(tmp_path, monkeypatch):
    data = {
        "primary_cluster": {
            "name": "primary",
            "mgmt_url": "http://default:17665/mgmt/bpl",
            "data_retrieval_url": "http://default:17668/retrieval",
        },
        "secondary_cluster": {
            "name": "secondary",
            "mgmt_url": "http://default:17665/mgmt/bpl",
            "data_retrieval_url": "http://default:17668/retrieval",
        },
    }
    monkeypatch.setenv("PRIMARY_MGMT_URL", "http://override:17665/mgmt/bpl")
    config = load_config(_write_config(tmp_path, data))
    assert config.primary.mgmt_url == "http://override:17665/mgmt/bpl"
