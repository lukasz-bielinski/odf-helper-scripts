# ODF / Ceph Storage Audit Toolkit

Audit and troubleshooting toolkit for OpenShift Data Foundation (Ceph) environments. The scripts run directly from a cloned repository (no installer) and provide:

- One-command full audit of Ceph RBD/CephFS/RGW plus Kubernetes PV/PVC/ObjectBucket relationships.
- JSON + text artifacts that describe what is consuming space and which resources look orphaned.
- Helper scripts for top-consumer views, interactive investigations, quick daily checks, and safe cleanup command generation.

## Requirements

- `oc` CLI logged into the target cluster (cluster-admin or storage-admin privileges).
- The `rook-ceph-tools` pod running in `openshift-storage` (create if missing).
- Local tooling: `jq`, `bc`, `awk`, `sed`, `grep`, `column`, `mail` (optional for notifications).

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

`./odf-audit.sh` now produces a structured dataset in one pass:

| Artifact | Description |
| --- | --- |
| `report.txt` | Human-readable report with cluster status, pool highlights, RBD/CephFS/RGW summaries, Kubernetes inventory, and cleanup hints |
| `SUMMARY.txt` | One-page executive summary |
| `report.json` | Machine-readable summary (cluster stats, datasets, orphan lists) |
| `data/` | Raw JSON captures (`ceph-status`, `rados-df`, `rbd-images.json`, `rgw-buckets.json`, `pv.json`, `pvc.json`, `obc.json`, `cephfs-subvols.json`, etc.) |

Major sections in `report.txt`:

1. **Cluster Overview** – Ceph health/usage, MON/OSD counts, top pools.
2. **RBD Pools & Images** – Inventory of every image with size, watcher count, PV linkage, and orphan detection (images with no PV + no watchers + “test/bench” heuristics).
3. **CephFS** – Filesystems, subvolume groups, and subvol counts.
4. **RGW Buckets** – Owner, size, object counts, Loki-focused flags, bucket ↔ OBC parity.
5. **Kubernetes Inventory** – PV/PVC/ObjectBucketClaim totals with bindings.
6. **Potential Cleanup** – Aggregated orphan lists and suspected benchmark/test pools.

## Orphan / Anomaly Detection

The audit highlights resources that likely deserve a manual review:

- **RBD images**: flagged if no Kubernetes PV references (`volumeHandle`) and no watchers; heuristic flag for names resembling `test/bench/temp/fio`.
- **RGW buckets**: flagged when no ObjectBucketClaim references exist. Loki-named buckets are grouped separately.
- **Pools**: any pool whose name matches `(test|bench|perf|fio|tmp|temp)` appears under “Suspected benchmark/test pools”.
- **Kubernetes**: counts of Pending/Failed PVs and total PVCs are logged to help detect leaks.

All flagged items are also emitted in `report.json.orphans.*` for downstream automation and are consumed by `odf-cleanup-generator.sh`.

## Companion Tools

| Script | Purpose |
| --- | --- |
| `odf-quick-check.sh` | Fast daily status (cluster health, capacity, OSD/PG status, high-level orphan estimate) |
| `odf-top-consumers.sh <AUDIT_DIR>` | Renders largest pools, RBD images, RGW buckets, biggest PVCs, namespace totals, and cleanup impact using the audit datasets |
| `odf-cleanup-generator.sh <AUDIT_DIR>` | Creates `cleanup-commands.sh` with **commented** Ceph/Kubernetes commands for every flagged orphan (still requires human verification) |
| `odf-inspector.sh` | Interactive TUI for drilling into specific RBD images, RGW buckets, PVs, PVCs, and pools |

## Safe Cleanup Workflow

1. Run `./odf-audit.sh` and inspect `report.txt` / `report.json`.
2. Generate suggestions: `./odf-cleanup-generator.sh $AUDIT_DIR`.
3. Review `cleanup-commands.sh` – every command remains commented.
4. For each candidate resource:
   - Re-run `rbd status`, `radosgw-admin bucket stats`, or `oc describe pv/pvc` as indicated in the file.
   - Take backups (if needed) and obtain production approvals.
5. Uncomment relevant commands one by one and execute manually.

## Automating

See `crontab-examples.txt` for ready-to-use schedules:

- Daily `odf-quick-check.sh` with email/slack notifications on warnings.
- Weekly `odf-audit.sh` + `odf-top-consumers.sh` to keep rolling reports.
- Monthly cleanup review that regenerates SAFE MODE commands.

## Troubleshooting

- **rook-ceph-tools pod missing** – `oc get pods -n openshift-storage | grep rook-ceph-tools`. Create it via the standard toolbox manifest if needed.
- **`jq` / `bc` missing** – install via your distro package manager.
- **Scripts hang** – exec into `rook-ceph-tools` and run `ceph -s` to verify responsiveness; overloaded clusters may slow down the audit.
- **Permission denied** – ensure your user can `oc auth can-i create pods/exec -n openshift-storage` and view PV/PVC/OB resources.

## Safety Checklist

- Treat `cleanup-commands.sh` as *read-only guidance* until every entry is validated.
- Never delete RBD images that still report watchers or PVC bindings.
- Loki buckets often have long retention; confirm decommissioning before purging.
- Keep historical audit directories for trend analysis and rollback notes.
- Run destructive actions in maintenance windows whenever possible.

Happy auditing! Contributions and issue reports are welcome.
