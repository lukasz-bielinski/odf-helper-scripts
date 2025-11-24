# ODF / Ceph Storage Audit Toolkit

Audit and troubleshooting toolkit for OpenShift Data Foundation (Ceph) environments. The scripts run directly from a cloned repository (no installer) and provide:

- One-command full audit of Ceph RBD/CephFS/RGW plus Kubernetes PV/PVC/ObjectBucket relationships.
- JSON + text artifacts that describe what is consuming space and which resources look orphaned.
- Helper scripts for top-consumer views, interactive investigations, quick daily checks, and safe cleanup command generation.

## Requirements

- `oc` CLI logged into the target cluster (cluster-admin or storage-admin privileges).
- The `rook-ceph-tools` pod running in `openshift-storage` (create if missing).
- Access to OpenShift monitoring (Prometheus/Thanos Querier) for enhanced metrics collection.
- Local tooling: `jq`, `bc`, `awk`, `sed`, `grep`, `column`, `curl`, `mail` (optional for notifications).

## Quick Start

```bash
git clone https://github.com/USER/odf-helper-scripts.git
cd odf-helper-scripts
chmod +x odf-*.sh

# 30-second snapshot
./odf-quick-check.sh

# Full audit (~3-5 minutes)
./odf-audit.sh
AUDIT_DIR=$(ls -td /tmp/odf-audit-* | head -1)

# Space drill-down
./odf-top-consumers.sh "$AUDIT_DIR"

# Generate SAFE MODE cleanup suggestions
./odf-cleanup-generator.sh "$AUDIT_DIR"
```

All artifacts land under `/tmp/odf-audit-YYYYMMDD-HHMMSS/` unless you override `ODF_AUDIT_DIR` or pass a custom output path.

## odf-audit.sh – What It Collects

`./odf-audit.sh` now produces a structured dataset in one pass, using **Prometheus metrics** as the primary data source:

| Artifact | Description |
| --- | --- |
| `report.txt` | Human-readable report with cluster status, pool highlights, RBD/CephFS/RGW summaries, Kubernetes inventory, and cleanup hints |
| `SUMMARY.txt` | One-page executive summary |
| `report.json` | Machine-readable summary (cluster stats, datasets, orphan lists, data sources) |
| `potential-orphans.txt` | Detailed orphan candidates report categorized by confidence level for manual review |
| `data/` | Raw JSON captures (Prometheus metrics: `prom-*.json`, Ceph commands: `ceph-status.json`, application data: `rbd-images.json`, `rgw-buckets.json`, `pv.json`, `pvc.json`, `obc.json`, `cephfs-subvols.json`, etc.) |

Major sections in `report.txt`:

1. **Cluster Overview** – Ceph health/usage, MON/OSD counts, raw capacity, used capacity, stored data, storage efficiency ratio, top pools by usage.
2. **RBD Pools & Images** – Inventory of every image with size, watcher count, PV linkage, test pattern detection, and enhanced orphan detection (images with no PV + no watchers + volumeHandle parsing).
3. **CephFS** – Filesystems, Prometheus metrics (total bytes/files managed), subvolume groups, and subvol counts.
4. **RGW Buckets** – Owner, size, object counts, dual OBC mapping (spec + secrets), Loki/logging flags, bucket ↔ OBC parity.
5. **Kubernetes Inventory** – PV/PVC/ObjectBucketClaim totals with bindings.
6. **Orphan Detection Summary** – High-confidence orphans (safe to review), medium-confidence (manual review required), and summary counts by category.

## Orphan / Anomaly Detection

The audit uses **enhanced heuristics** to categorize potential orphans by confidence level:

### High Confidence Orphans
- **RBD images**: No Kubernetes PV, no watchers, contains data. Enhanced volumeHandle parsing for accurate PV matching.
- **RGW buckets**: No ObjectBucketClaim (checked both spec and secrets), not Loki/logging related.

### Medium Confidence (Manual Review Required)
- **RBD images**: No PV but has active watchers (may be stale).
- **RGW buckets**: Loki/logging related (may be operational).
- **Test patterns**: Images with test/bench/fio/temp names.

### Data Sources
- **Prometheus metrics**: Used for accurate capacity/usage data (no unit guessing).
- **Dual OBC mapping**: Checks both `spec.bucketName` and Secret bucket names for complete coverage.
- **VolumeHandle parsing**: Extracts pool/image from CSI volumeHandle format for accurate matching.

All flagged items are emitted in `report.json.orphans.*` with categorization and consumed by `odf-cleanup-generator.sh`. A detailed human-readable `potential-orphans.txt` is also generated for manual review.

## Companion Tools

| Script | Purpose |
| --- | --- |
| `odf-quick-check.sh` | Fast daily status (cluster health, capacity, OSD/PG status, high-level orphan estimate) |
| `odf-top-consumers.sh <AUDIT_DIR>` | Renders largest pools (prefers Prometheus metrics), RBD images, RGW buckets, biggest PVCs, namespace totals, and cleanup impact using the audit datasets |
| `odf-cleanup-generator.sh <AUDIT_DIR>` | Creates `cleanup-commands.sh` with **commented** Ceph/Kubernetes commands for high-confidence orphans, with warnings for medium-confidence items (still requires human verification) |
| `odf-inspector.sh` | Interactive TUI for drilling into specific RBD images, RGW buckets, PVs, PVCs, and pools |

## Safe Cleanup Workflow

1. Run `./odf-audit.sh` and inspect `report.txt` / `report.json` / `potential-orphans.txt`.
2. **Review `potential-orphans.txt`** - this detailed report categorizes orphans by confidence level with verification steps.
3. Generate cleanup commands: `./odf-cleanup-generator.sh $AUDIT_DIR`.
4. Review `cleanup-commands.sh` – every command remains commented, with high-confidence orphans listed first and medium-confidence items as warnings.
5. For each candidate resource:
   - Follow verification steps in `potential-orphans.txt`.
   - Re-run `rbd status`, `radosgw-admin bucket stats`, or `oc describe pv/pvc` as indicated.
   - Check cluster events: `oc get events -A | grep <resource-name>`.
   - Wait 7 days and re-run audit to confirm resource is still orphaned.
   - Take backups (if needed) and obtain production approvals.
6. Uncomment relevant commands one by one and execute manually in a maintenance window.

## Automating

See `crontab-examples.txt` for ready-to-use schedules:

- Daily `odf-quick-check.sh` with email/slack notifications on warnings.
- Weekly `odf-audit.sh` + `odf-top-consumers.sh` to keep rolling reports.
- Monthly cleanup review that regenerates SAFE MODE commands.

## Troubleshooting

- **rook-ceph-tools pod missing** – `oc get pods -n openshift-storage | grep rook-ceph-tools`. Create it via the standard toolbox manifest if needed.
- **Prometheus metrics unavailable** – ensure you have access to `openshift-monitoring` namespace and Thanos Querier route. Scripts will fall back to `ceph df` commands if Prometheus is not accessible.
- **`jq` / `bc` / `curl` missing** – install via your distro package manager.
- **Scripts hang** – exec into `rook-ceph-tools` and run `ceph -s` to verify responsiveness; overloaded clusters may slow down the audit.
- **Permission denied** – ensure your user can `oc auth can-i create pods/exec -n openshift-storage`, view PV/PVC/OB resources, and access monitoring endpoints (`oc whoami -t` should return a token).

## Safety Checklist

- Treat `cleanup-commands.sh` as *read-only guidance* until every entry is validated.
- Never delete RBD images that still report watchers or PVC bindings.
- Loki buckets often have long retention; confirm decommissioning before purging.
- Keep historical audit directories for trend analysis and rollback notes.
- Run destructive actions in maintenance windows whenever possible.

Happy auditing! Contributions and issue reports are welcome.
