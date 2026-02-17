"""Configuration loader for the archiver orchestrator."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class ClusterConfig:
    name: str
    mgmt_url: str
    data_retrieval_url: str


@dataclass
class FailoverConfig:
    strategy: str = "dns"
    dns_record: str = ""
    auto_failover: bool = False
    auto_failover_delay: int = 120
    auto_failback: bool = False


@dataclass
class ReconciliationConfig:
    auto_reconcile: bool = False
    max_concurrent_merges: int = 1
    merge_target_store: str = "LTS"
    # How many days the secondary's MTS retains data before blackhole ETL
    # discards it. Used to alert operators when an unreconciled outage is
    # approaching the deadline. Should match the secondary's MTS hold value
    # minus gather (e.g., hold=30, gather=1 → 29 days retention).
    secondary_retention_days: int = 29


@dataclass
class AlertingConfig:
    webhook_url: str = ""


@dataclass
class OrchestratorConfig:
    sync_interval: int = 300
    health_check_interval: int = 30
    archive_batch_size: int = 100
    batch_delay: int = 5
    api_port: int = 9090
    state_file: str = "/var/lib/archiver-orchestrator/state.json"

    primary: ClusterConfig = field(default_factory=lambda: ClusterConfig("primary", "", ""))
    secondary: ClusterConfig = field(default_factory=lambda: ClusterConfig("secondary", "", ""))
    failover: FailoverConfig = field(default_factory=FailoverConfig)
    reconciliation: ReconciliationConfig = field(default_factory=ReconciliationConfig)
    alerting: AlertingConfig = field(default_factory=AlertingConfig)


def load_config(config_path: str | Path) -> OrchestratorConfig:
    """Load configuration from a YAML file."""
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    orch = raw.get("orchestrator", {})
    primary = raw.get("primary_cluster", {})
    secondary = raw.get("secondary_cluster", {})
    failover = raw.get("failover", {})
    reconciliation = raw.get("reconciliation", {})
    alerting = raw.get("alerting", {})

    # Allow environment variable overrides
    primary["mgmt_url"] = os.environ.get(
        "PRIMARY_MGMT_URL", primary.get("mgmt_url", "")
    )
    primary["data_retrieval_url"] = os.environ.get(
        "PRIMARY_RETRIEVAL_URL", primary.get("data_retrieval_url", "")
    )
    secondary["mgmt_url"] = os.environ.get(
        "SECONDARY_MGMT_URL", secondary.get("mgmt_url", "")
    )
    secondary["data_retrieval_url"] = os.environ.get(
        "SECONDARY_RETRIEVAL_URL", secondary.get("data_retrieval_url", "")
    )

    return OrchestratorConfig(
        sync_interval=orch.get("sync_interval", 300),
        health_check_interval=orch.get("health_check_interval", 30),
        archive_batch_size=orch.get("archive_batch_size", 100),
        batch_delay=orch.get("batch_delay", 5),
        api_port=orch.get("api_port", 9090),
        state_file=orch.get("state_file", "/var/lib/archiver-orchestrator/state.json"),
        primary=ClusterConfig(
            name=primary.get("name", "primary"),
            mgmt_url=primary.get("mgmt_url", ""),
            data_retrieval_url=primary.get("data_retrieval_url", ""),
        ),
        secondary=ClusterConfig(
            name=secondary.get("name", "secondary"),
            mgmt_url=secondary.get("mgmt_url", ""),
            data_retrieval_url=secondary.get("data_retrieval_url", ""),
        ),
        failover=FailoverConfig(
            strategy=failover.get("strategy", "dns"),
            dns_record=failover.get("dns_record", ""),
            auto_failover=failover.get("auto_failover", False),
            auto_failover_delay=failover.get("auto_failover_delay", 120),
            auto_failback=failover.get("auto_failback", False),
        ),
        reconciliation=ReconciliationConfig(
            auto_reconcile=reconciliation.get("auto_reconcile", False),
            max_concurrent_merges=reconciliation.get("max_concurrent_merges", 1),
            merge_target_store=reconciliation.get("merge_target_store", "LTS"),
            secondary_retention_days=reconciliation.get("secondary_retention_days", 29),
        ),
        alerting=AlertingConfig(
            webhook_url=alerting.get("webhook_url", ""),
        ),
    )
