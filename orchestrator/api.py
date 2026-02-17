"""Status API module — REST API for operators and Prometheus metrics."""

import asyncio
import logging
import time

from aiohttp import web
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from config import OrchestratorConfig
from failover_director import FailoverDirector
from health_monitor import ClusterState, HealthMonitor
from pv_sync import PVSync
from reconciliation import Reconciliation

logger = logging.getLogger(__name__)

# Prometheus metrics
HEALTH_CHECK_GAUGE = Gauge(
    "archiver_cluster_healthy", "Whether the cluster is healthy (1=yes, 0=no)", ["cluster"]
)
CLUSTER_STATE_GAUGE = Gauge(
    "archiver_cluster_state", "Cluster state enum value", ["cluster", "state"]
)
CLUSTER_PV_COUNT = Gauge(
    "archiver_cluster_pv_count", "Total PV count for cluster", ["cluster"]
)
SYNC_COUNTER = Counter(
    "archiver_sync_total", "Total sync operations", ["result"]
)
FAILOVER_COUNTER = Counter(
    "archiver_failover_total", "Total failover events", ["action"]
)
RECONCILIATION_GAUGE = Gauge(
    "archiver_reconciliation_progress", "Reconciliation progress", ["metric"]
)


def build_app(
    config: OrchestratorConfig,
    health_monitor: HealthMonitor,
    pv_sync: PVSync,
    failover_director: FailoverDirector,
    reconciliation: Reconciliation,
) -> web.Application:
    """Build the aiohttp application with all routes."""

    app = web.Application()
    app["config"] = config
    app["health"] = health_monitor
    app["sync"] = pv_sync
    app["failover"] = failover_director
    app["reconciliation"] = reconciliation

    app.router.add_get("/api/v1/status", handle_status)
    app.router.add_get("/api/v1/clusters", handle_clusters)
    app.router.add_get("/api/v1/sync/status", handle_sync_status)
    app.router.add_post("/api/v1/sync/trigger", handle_sync_trigger)
    app.router.add_post("/api/v1/failover/switch", handle_failover_switch)
    app.router.add_get("/api/v1/reconciliation/status", handle_reconciliation_status)
    app.router.add_post("/api/v1/reconciliation/trigger", handle_reconciliation_trigger)
    app.router.add_get("/metrics", handle_metrics)

    return app


async def handle_status(request: web.Request) -> web.Response:
    health: HealthMonitor = request.app["health"]
    failover: FailoverDirector = request.app["failover"]
    sync: PVSync = request.app["sync"]

    return web.json_response(
        {
            "active_cluster": failover.active_cluster,
            "primary": {
                "state": health.primary_health.state.value,
                "total_pvs": health.primary_health.total_pvs,
                "last_check": health.primary_health.last_check,
            },
            "secondary": {
                "state": health.secondary_health.state.value,
                "total_pvs": health.secondary_health.total_pvs,
                "last_check": health.secondary_health.last_check,
            },
            "last_sync": {
                "added": len(sync.last_result.added) if sync.last_result else 0,
                "removed": len(sync.last_result.removed) if sync.last_result else 0,
            },
        }
    )


async def handle_clusters(request: web.Request) -> web.Response:
    health: HealthMonitor = request.app["health"]

    def _health_to_dict(h):
        return {
            "name": h.name,
            "state": h.state.value,
            "total_appliances": h.total_appliances,
            "active_appliances": h.active_appliances,
            "total_pvs": h.total_pvs,
            "pv_status": h.pv_status,
            "last_check": h.last_check,
            "last_healthy": h.last_healthy,
            "outage_start": h.outage_start,
            "outage_end": h.outage_end,
            "error": h.error,
        }

    return web.json_response(
        {
            "primary": _health_to_dict(health.primary_health),
            "secondary": _health_to_dict(health.secondary_health),
        }
    )


async def handle_sync_status(request: web.Request) -> web.Response:
    sync: PVSync = request.app["sync"]
    result = sync.last_result

    if result is None:
        return web.json_response({"status": "no sync has run yet"})

    return web.json_response(
        {
            "added": result.added,
            "removed": result.removed,
            "failed_adds": result.failed_adds,
            "failed_removes": result.failed_removes,
            "total_added": len(result.added),
            "total_removed": len(result.removed),
            "total_failed": len(result.failed_adds) + len(result.failed_removes),
        }
    )


async def handle_sync_trigger(request: web.Request) -> web.Response:
    sync: PVSync = request.app["sync"]
    asyncio.create_task(sync.run_sync())
    return web.json_response({"status": "sync triggered"})


async def handle_failover_switch(request: web.Request) -> web.Response:
    failover: FailoverDirector = request.app["failover"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON body"}, status=400)

    target = body.get("target")
    if target not in ("primary", "secondary"):
        return web.json_response(
            {"error": "target must be 'primary' or 'secondary'"}, status=400
        )

    ok = await failover.manual_switch(target)
    if ok:
        return web.json_response({"status": f"switched to {target}"})
    return web.json_response({"error": "failover switch failed"}, status=500)


async def handle_reconciliation_status(request: web.Request) -> web.Response:
    recon: Reconciliation = request.app["reconciliation"]
    p = recon.progress
    return web.json_response(
        {
            "running": p.running,
            "total_pvs": p.total_pvs,
            "completed_pvs": p.completed_pvs,
            "failed_pvs": p.failed_pvs,
            "start_time": p.start_time,
            "end_time": p.end_time,
            "outage_start": p.outage_start,
            "outage_end": p.outage_end,
        }
    )


async def handle_reconciliation_trigger(request: web.Request) -> web.Response:
    recon: Reconciliation = request.app["reconciliation"]

    outage_start = None
    outage_end = None
    try:
        body = await request.json()
        outage_start = body.get("outage_start")
        outage_end = body.get("outage_end")
    except Exception:
        pass

    asyncio.create_task(recon.run_reconciliation(outage_start, outage_end))
    return web.json_response({"status": "reconciliation triggered"})


async def handle_metrics(request: web.Request) -> web.Response:
    """Prometheus-compatible metrics endpoint."""
    health: HealthMonitor = request.app["health"]

    for cluster_name, cluster_health in [
        ("primary", health.primary_health),
        ("secondary", health.secondary_health),
    ]:
        HEALTH_CHECK_GAUGE.labels(cluster=cluster_name).set(
            1 if cluster_health.state == ClusterState.HEALTHY else 0
        )
        CLUSTER_PV_COUNT.labels(cluster=cluster_name).set(cluster_health.total_pvs)
        for state in ClusterState:
            CLUSTER_STATE_GAUGE.labels(cluster=cluster_name, state=state.value).set(
                1 if cluster_health.state == state else 0
            )

    body = generate_latest()
    return web.Response(body=body, content_type="text/plain; version=0.0.4; charset=utf-8")
