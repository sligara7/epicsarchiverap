# Dual-Cluster Archival Resilience

## Problem

The EPICS Archiver Appliance provides horizontal scaling via clustering, but no
archival resilience within a cluster. If a node goes down, the PVs assigned to
that node stop being archived with no automatic failover. The existing
`ReassignAppliance` BPL cannot help because it requires all nodes to be up
(`hasClusterFinishedInitialization`) — the exact condition that fails when a
node is down.

## Solution

Deploy two completely independent archiver clusters — a **primary** and a
**secondary** — with an external orchestrator that synchronizes PV lists,
monitors health, and manages client failover.

Both clusters independently subscribe to the same EPICS PVs via CA/PVA (the
protocol allows unlimited concurrent subscriptions). Each cluster writes to its
own storage. If the primary has a node failure, the secondary has been capturing
those same samples the entire time — zero data gap.

## Architecture

```
                 ┌─────────────────────┐
                 │    Orchestrator      │
                 │  (Python service)    │
                 │  - PV sync           │
                 │  - Health monitoring  │
                 │  - Failover control   │
                 └──────┬───────┬───────┘
                        │       │
          ┌─────────────┘       └──────────────┐
          ▼                                    ▼
┌──────────────────┐              ┌──────────────────┐
│ Primary Cluster  │◄── queries ──┤ Secondary Cluster │
│ (N nodes)        │  for merge   │ (M nodes)         │
│ STS → MTS → LTS  │              │ STS → MTS → ∅     │
│ (permanent store)│              │ (safety net only)  │
└──────────────────┘              └──────────────────┘
          │                                │
          └────────── Both connect ────────┘
                    to EPICS IOCs
```

## Storage Strategy

The primary and secondary clusters do **not** share storage. Duplicating
long-term storage across both clusters would be wasteful — at scale (30–60
beamlines, millions of PVs), LTS is the dominant storage cost.

Instead:

| Tier | Primary Cluster | Secondary Cluster |
|------|----------------|-------------------|
| **STS** (Short-Term) | `pb://` with `PARTITION_HOUR` | `pb://` with `PARTITION_HOUR` |
| **MTS** (Medium-Term) | `pb://` with `PARTITION_DAY` | `pb://` with `PARTITION_DAY`, `hold=30`, `gather=1` |
| **LTS** (Long-Term) | `pb://` with `PARTITION_YEAR` | `blackhole://localhost?name=LTS` |

- **Primary** runs the full STS → MTS → LTS pipeline. LTS is the permanent
  record of all archived data.
- **Secondary** runs STS → MTS → blackhole. Data that ages out of MTS (~29 days
  with `hold=30, gather=1`) is discarded. The secondary is a safety net, not a
  second permanent archive.

For retrieval, the secondary is configured with `mergeDuringRetrieval` pointing
at the primary. This is a feature already built into the archiver appliance. In
plain terms: when a client queries the secondary for data (e.g., "give me PV X
from January to March"), the secondary doesn't just look in its own storage. It
also makes the same request to the primary behind the scenes, gets both sets of
data, merges them together, and returns the combined result. This merge is
read-only — it does not change any stored data on either cluster. If the primary
is unreachable, the secondary just returns whatever it has locally and degrades
gracefully.

The net effect: a client querying the secondary sees the **same complete
dataset** as if they queried the primary, even though the secondary only stores
~29 days of data locally. The primary's LTS fills in all the historical data
transparently.

### Example secondary `policies.py` dataStores

```python
pvPolicyDict['dataStores'] = [
    'pb://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}'
    '&partitionGranularity=PARTITION_HOUR&consolidateOnShutdown=true',

    'pb://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}'
    '&partitionGranularity=PARTITION_DAY&hold=30&gather=1',

    'blackhole://localhost?name=LTS'
]
```

The `hold=30` on daily partitions means the secondary retains ~29 days of data
in MTS before the blackhole discards it. This is your **reconciliation window**.

## How It Works

### Normal Operation

1. The orchestrator syncs PV lists from primary → secondary every 5 minutes.
2. Both clusters independently archive the same PVs.
3. Clients query the primary. The secondary sits idle from a retrieval
   perspective.

### Primary Node Failure

1. A node in the primary cluster goes down.
2. PVs assigned to that node stop being archived **on the primary** — there is a
   gap in the primary's data.
3. The secondary is unaffected. It subscribed to those same PVs independently
   and continues archiving them into its own STS/MTS.
4. The orchestrator detects the primary is degraded/down and can optionally
   redirect clients to the secondary for retrieval.

### After the Primary Node Recovers

1. The primary node comes back and resumes archiving (new data flowing).
2. The orchestrator detects recovery and triggers **reconciliation**.
3. For each PV that had a gap during the outage:
   - **Pause** archiving for that PV on the primary
   - **mergeInData** pulls gap data from the secondary's retrieval endpoint into
     the primary's LTS
   - **Resume** archiving for that PV on the primary
4. The primary's LTS now has the complete record — no gap.
5. The secondary's copy of that gap data eventually ETLs to the blackhole and
   gets discarded. It served its purpose.

### Secondary Node Failure

A secondary node failure **does not require reconciliation**. The secondary is
not the system of record:

- The primary has the complete permanent record in LTS. No data is lost.
- Even if you reconciled the secondary from the primary, the data would just
  get blackholed in a few weeks anyway.
- A secondary outage only degrades your resilience temporarily — the safety
  net has a hole until the secondary node recovers.

The orchestrator tracks secondary health and alerts operators so they know not
to perform planned primary maintenance while the safety net is compromised.

The one edge case: if a secondary node fails, recovers, and then the primary
fails for the **same PVs** during a window that overlaps the secondary's earlier
gap, the secondary wouldn't have data for that overlap. This is a double-failure
scenario — unlikely, and the orchestrator's alerting makes it visible.

## Reconciliation Deadline

Because the secondary uses blackhole LTS, gap data in MTS has a finite
lifetime. The `secondary_retention_days` config setting (default: 29 days) tells
the orchestrator how long that data survives.

The orchestrator alerts at **75% of the retention window** if reconciliation
hasn't run. For example, with 29-day retention, you get a warning at ~22 days
after an unreconciled outage.

**If reconciliation doesn't happen before the retention window expires, the gap
data is lost.** Set `hold` high enough to cover your worst-case outage +
recovery + reconciliation time.

## BPL Endpoints Added

### `GET /mgmt/bpl/getClusterHealthSummary`

Returns a single JSON object with cluster-wide health:

```json
{
  "clusterHealthy": true,
  "totalAppliances": 3,
  "activeAppliances": 3,
  "totalPVs": 150000,
  "totalEventRate": 45000.0,
  "totalStorageRate": 15000000.0,
  "pvsByStatus": {
    "Being archived": 149500,
    "Paused": 200
  },
  "appliances": [
    {
      "identity": "appliance0",
      "reachable": true,
      "pvCount": 50000,
      "eventRate": 15000.0,
      "storageRate": 5000000.0
    }
  ]
}
```

### `GET /mgmt/bpl/exportArchivalConfig`

Returns slimmed-down PV configs (no appliance-specific fields):

```json
[
  {
    "pvName": "LINAC:QUAD:1:CURRENT",
    "samplingPeriod": 1.0,
    "samplingMethod": "MONITOR",
    "policyName": "Default",
    "archiveFields": ["HIHI", "LOLO"],
    "usePVAccess": false,
    "paused": false
  }
]
```

## Orchestrator Modules

| Module | Purpose |
|--------|---------|
| `pv_sync.py` | Syncs PV lists primary → secondary (batched, with status polling) |
| `health_monitor.py` | Tracks per-cluster state: HEALTHY → DEGRADED → DOWN → RECOVERING |
| `failover_director.py` | DNS or proxy-based client failover (auto or manual) |
| `reconciliation.py` | Merges secondary gap data into primary LTS after outages |
| `api.py` | REST API for operators + Prometheus `/metrics` endpoint |
| `main.py` | Service entry point, background loops, state persistence |
| `config.py` | Configuration loading with environment variable overrides |

## Secondary Cluster Setup

### One-time: Register primary as external failover server

```bash
curl "http://secondary-mgmt:17665/mgmt/bpl/addExternalArchiverServer?\
externalarchiverserverurl=http://primary-retrieval:17668/retrieval/bpl?mergeDuringRetrieval=true&\
externalServerType=ARCHAPPL_PBRAW"

curl "http://secondary-mgmt:17665/mgmt/bpl/addExternalArchiverServerArchives?\
externalarchiverserverurl=http://primary-retrieval:17668/retrieval/bpl?mergeDuringRetrieval=true&\
archives=pbraw"
```

These two curl commands tell the secondary cluster: "the primary cluster also
has archived data — whenever someone asks you for data, also ask the primary for
the same data and merge the results together before responding." This is the
`mergeDuringRetrieval` feature built into the archiver appliance. It is
read-only and does not modify stored data on either side. If the primary is
unreachable at query time, the secondary simply returns its own local data.

## Running the Orchestrator

```bash
pip install -r requirements.txt
python main.py -c config.yaml
```

With verbose logging:

```bash
python main.py -c config.yaml -v
```

### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/status` | Overall system status |
| GET | `/api/v1/clusters` | Both clusters' health details |
| GET | `/api/v1/sync/status` | Last sync result |
| POST | `/api/v1/sync/trigger` | Manually trigger PV sync |
| POST | `/api/v1/failover/switch` | Manually trigger failover (`{"target": "secondary"}`) |
| GET | `/api/v1/reconciliation/status` | Reconciliation progress |
| POST | `/api/v1/reconciliation/trigger` | Manually start reconciliation |
| GET | `/metrics` | Prometheus-compatible metrics |

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| Secondary down during sync | Orchestrator retries next interval; primary unaffected |
| Both clusters healthy, network partitioned from each other | Both archive independently; secondary's retrieval merge with the primary gracefully degrades (returns local data only); data reconciled when partition heals |
| Orchestrator crashes | Both clusters continue archiving independently; PV sync pauses; deploy with systemd/k8s for auto-restart |
| PV added to primary during secondary outage | Synced on next successful interval after secondary recovers |
| Duplicate archivePV call on secondary | Returns "Already submitted" — not an error |
| Secondary node failure | No reconciliation needed — primary has complete data in LTS |
| Unreconciled outage approaching retention deadline | Orchestrator alerts at 75% of secondary_retention_days |
