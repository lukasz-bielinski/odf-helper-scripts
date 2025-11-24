#!/bin/bash
#
# ODF/Ceph Top Space Consumers Analyzer
#

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <audit-report-directory>"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

require_commands jq column bc oc

AUDIT_DIR="$1"
REPORT_FILE="${AUDIT_DIR}/top-consumers-report.txt"
DATA_DIR="$AUDIT_DIR/data"
JSON_REPORT="$AUDIT_DIR/report.json"

if [[ ! -d "$AUDIT_DIR" ]]; then
    echo "ERROR: Audit directory not found: $AUDIT_DIR"
    exit 1
fi

POOLS_JSON="$DATA_DIR/rados-df.json"
RBD_JSON="$DATA_DIR/rbd-images.json"
RGW_JSON="$DATA_DIR/rgw-buckets.json"
PVC_JSON="$DATA_DIR/pvc.json"
REPORT_JSON_AVAILABLE=false
if [[ -f "$JSON_REPORT" ]]; then
    REPORT_JSON_AVAILABLE=true
fi

echo "=== ODF TOP SPACE CONSUMERS REPORT ===" | tee "$REPORT_FILE"
echo "Generated: $(date)" | tee -a "$REPORT_FILE"
echo "" | tee -a "$REPORT_FILE"

print_pools() {
    echo "=== 1. POOLS BY SIZE (Largest First) ===" | tee -a "$REPORT_FILE"
    if [[ -f "$POOLS_JSON" ]]; then
        jq -r '.pools[]? | [.name, (.stats.kb_used/1024)] | @tsv' "$POOLS_JSON" \
            | sort -k2 -n -r | head -15 \
            | awk '{printf "  %-35s %12.2f MiB\n", $1, $2}' | tee -a "$REPORT_FILE"
    else
        ensure_rook_tools_pod
        rook_exec rados df | grep -v "^POOL_NAME" | grep -v "^---" | grep -v "^total_" \
            | sort -k3 -h -r | head -15 | tee -a "$REPORT_FILE"
    fi
    echo "" | tee -a "$REPORT_FILE"
}

print_top_rbd() {
    echo "=== 2. TOP RBD IMAGES BY SIZE ===" | tee -a "$REPORT_FILE"
    if [[ -f "$RBD_JSON" ]]; then
        jq -r '.[] | [.pool + "/" + .image, .size_bytes] | @tsv' "$RBD_JSON" \
            | sort -k2 -n -r | head -20 \
            | awk '{printf "  %-60s %12s\n", $1, sprintf("%.2f GiB", $2/1024/1024/1024)}' | tee -a "$REPORT_FILE"
    else
        echo "  (RBD dataset missing - run odf-audit.sh first)" | tee -a "$REPORT_FILE"
    fi
    echo "" | tee -a "$REPORT_FILE"
}

print_top_rgw() {
    echo "=== 3. TOP RGW BUCKETS BY SIZE ===" | tee -a "$REPORT_FILE"
    if [[ -f "$RGW_JSON" ]]; then
        jq -r '.[] | [.name, .size_bytes, .objects] | @tsv' "$RGW_JSON" \
            | sort -k2 -n -r | head -20 \
            | awk '{printf "  %-45s %12s (%s objects)\n", $1, sprintf("%.2f GiB", $2/1024/1024/1024), $3}' | tee -a "$REPORT_FILE"
    else
        echo "  (RGW dataset missing - run odf-audit.sh first)" | tee -a "$REPORT_FILE"
    fi
    echo "" | tee -a "$REPORT_FILE"
}

convert_size() {
    local size=$1
    local value=${size//[^0-9.]/}
    local unit=${size//[0-9.]/}
    local multiplier=1
    case "$unit" in
        Ti) multiplier=1099511627776 ;;
        Gi) multiplier=1073741824 ;;
        Mi) multiplier=1048576 ;;
        Ki) multiplier=1024 ;;
    esac
    printf '%.0f\n' "$(echo "$value * $multiplier" | bc -l)"
}

print_top_pvcs() {
    echo "=== 4. TOP PVCs BY REQUESTED SIZE ===" | tee -a "$REPORT_FILE"
    local source_json="$PVC_JSON"
    if [[ ! -f "$source_json" ]]; then
        echo "  (PVC dataset missing - reading live cluster)" | tee -a "$REPORT_FILE"
        oc get pvc --all-namespaces -o json > /tmp/odf-pvc.$$.json
        source_json="/tmp/odf-pvc.$$.json"
    fi

    jq -r '.items[] | select(.spec.resources.requests.storage) | "\(.spec.resources.requests.storage)|\(.metadata.namespace)|\(.metadata.name)|\(.spec.volumeName // \"pending\")"' "$source_json" \
        | while IFS='|' read -r size namespace name pv; do
            bytes=$(convert_size "$size")
            echo "$bytes|$namespace/$name|$size|$pv"
        done | sort -t'|' -k1 -n -r | head -20 \
        | while IFS='|' read -r bytes pvc size pv; do
            printf "  %-50s %15s (PV: %s)\n" "$pvc" "$size" "$pv"
        done | tee -a "$REPORT_FILE"

    [[ -f "/tmp/odf-pvc.$$.json" ]] && rm -f "/tmp/odf-pvc.$$.json"
    echo "" | tee -a "$REPORT_FILE"
}

print_namespace_aggregation() {
    echo "=== 5. TOP NAMESPACES BY TOTAL PVC SIZE ===" | tee -a "$REPORT_FILE"
    local source_json="$PVC_JSON"
    if [[ ! -f "$source_json" ]]; then
        oc get pvc --all-namespaces -o json > /tmp/odf-pvc.$$.json
        source_json="/tmp/odf-pvc.$$.json"
    fi

    jq -r '.items[] | select(.spec.resources.requests.storage) | "\(.metadata.namespace)|\(.spec.resources.requests.storage)"' "$source_json" \
        | while IFS='|' read -r namespace size; do
            bytes=$(convert_size "$size")
            echo "$namespace $bytes"
        done | awk '{sum[$1]+=$2} END {for (ns in sum) print sum[ns], ns}' \
        | sort -n -r | head -15 \
        | while read -r bytes namespace; do
            printf "  %-40s %12s\n" "$namespace" "$(human_bytes "$bytes")"
        done | tee -a "$REPORT_FILE"

    [[ -f "/tmp/odf-pvc.$$.json" ]] && rm -f "/tmp/odf-pvc.$$.json"
    echo "" | tee -a "$REPORT_FILE"
}

print_cleanup_impact() {
    echo "=== 6. POTENTIAL SPACE SAVINGS FROM CLEANUP ===" | tee -a "$REPORT_FILE"
    if [[ "$REPORT_JSON_AVAILABLE" == true ]]; then
        jq -r '
            "  RBD orphaned images: \(.orphans.rbd | length)",
            "  RGW orphaned buckets: \(.orphans.rgw | length)",
            "  Suspected benchmark/test pools: \(.orphans.suspected_pools | length)"
        ' "$JSON_REPORT" | tee -a "$REPORT_FILE"
    else
        echo "  (report.json missing - rerun odf-audit.sh)" | tee -a "$REPORT_FILE"
    fi
    echo "" | tee -a "$REPORT_FILE"
}

print_storage_breakdown() {
    echo "=== 7. STORAGE TYPE BREAKDOWN ===" | tee -a "$REPORT_FILE"
    if [[ "$REPORT_JSON_AVAILABLE" == true ]]; then
        jq -r '
            "  PVs total: \(.stats.pv_total) | PVCs: \(.stats.pvc_total) | OBCs: \(.stats.obc_total)",
            "  RBD images tracked: \(.stats.rbd_images)",
            "  RGW buckets tracked: \(.stats.rgw_buckets)"
        ' "$JSON_REPORT" | tee -a "$REPORT_FILE"
    else
        echo "  (report.json missing - rerun odf-audit.sh)" | tee -a "$REPORT_FILE"
    fi
    echo "" | tee -a "$REPORT_FILE"
}

print_pools
print_top_rbd
print_top_rgw
print_top_pvcs
print_namespace_aggregation
print_cleanup_impact
print_storage_breakdown

echo "Report saved to: $REPORT_FILE" | tee -a "$REPORT_FILE"
