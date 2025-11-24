#!/bin/bash
#
# Generates SAFE MODE cleanup suggestions from audit artifacts.
#

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <audit-report-directory>"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

require_commands jq oc bc

AUDIT_DIR="$1"
CLEANUP_SCRIPT="${AUDIT_DIR}/cleanup-commands.sh"
JSON_REPORT="$AUDIT_DIR/report.json"
JSON_AVAILABLE=false
if [[ -f "$JSON_REPORT" ]]; then
    JSON_AVAILABLE=true
fi

if [[ ! -d "$AUDIT_DIR" ]]; then
    echo "ERROR: Audit directory not found: $AUDIT_DIR"
    exit 1
fi

ensure_rook_tools_pod

cat <<EOF > "$CLEANUP_SCRIPT"
# ODF/Ceph Cleanup Commands
# Generated: $(date)
# Based on audit from: $AUDIT_DIR
#
# ⚠️  REVIEW CAREFULLY BEFORE EXECUTING
# ⚠️  ENSURE BACKUPS ARE TAKEN
# ⚠️  GET APPROVAL FOR PRODUCTION

set -euo pipefail

ROOK_TOOLS_POD="${ROOK_TOOLS_POD}"

EOF

cat <<'EOF' >> "$CLEANUP_SCRIPT"
# =============================================================================
# ORPHANED RBD IMAGES
# =============================================================================

EOF

if [[ "$JSON_AVAILABLE" == true ]]; then
    rbd_count=$(jq '.orphans.rbd | length' "$JSON_REPORT")
    if [[ "$rbd_count" -gt 0 ]]; then
        echo "# Found potentially orphaned RBD images:" >> "$CLEANUP_SCRIPT"
        jq -c '.orphans.rbd[]' "$JSON_REPORT" | while read -r row; do
            pool=$(echo "$row" | jq -r '.pool')
            image=$(echo "$row" | jq -r '.image')
            size=$(echo "$row" | jq -r '.size_human // "unknown"')
            cat <<EOF >> "$CLEANUP_SCRIPT"

# Image: $pool/$image ($size)
# Verify not in use: oc rsh -n openshift-storage \$ROOK_TOOLS_POD rbd status $pool/$image
# Check watchers before deletion!
# oc rsh -n openshift-storage \$ROOK_TOOLS_POD rbd rm $pool/$image

EOF
        done
    else
        echo "# No orphaned RBD images detected" >> "$CLEANUP_SCRIPT"
    fi
    echo "" >> "$CLEANUP_SCRIPT"
else
    if grep -q "ORPHANED?: " "$AUDIT_DIR/report.txt"; then
        echo "# Found potentially orphaned RBD images:" >> "$CLEANUP_SCRIPT"
        grep "ORPHANED?: " "$AUDIT_DIR/report.txt" | while read -r line; do
            image_path=$(echo "$line" | sed 's/.*ORPHANED?: //' | xargs)
            pool=$(echo "$image_path" | cut -d'/' -f1)
            image=$(echo "$image_path" | cut -d'/' -f2)
            cat <<EOF >> "$CLEANUP_SCRIPT"

# Image: $image_path
# Verify not in use: oc rsh -n openshift-storage \$ROOK_TOOLS_POD rbd status $pool/$image
# Check watchers before deletion!
# oc rsh -n openshift-storage \$ROOK_TOOLS_POD rbd rm $pool/$image

EOF
        done
    else
        echo "# No orphaned RBD images detected" >> "$CLEANUP_SCRIPT"
    fi
    echo "" >> "$CLEANUP_SCRIPT"
fi

# =============================================================================
# Analyze orphaned RGW buckets
# =============================================================================
cat <<'EOF' >> "$CLEANUP_SCRIPT"
# =============================================================================
# ORPHANED RGW BUCKETS
# =============================================================================

EOF

if [[ "$JSON_AVAILABLE" == true ]]; then
    jq -c '.orphans.rgw[]?' "$JSON_REPORT" | while read -r row; do
        bucket=$(echo "$row" | jq -r '.name')
        size=$(echo "$row" | jq -r '.size_bytes')
        objects=$(echo "$row" | jq -r '.objects')
        cat <<EOF >> "$CLEANUP_SCRIPT"

# Bucket: $bucket
# Owner: $(echo "$row" | jq -r '.owner') | Objects: $objects | Size: $(human_bytes "$size")
# Verify bucket contents: oc rsh -n openshift-storage \$ROOK_TOOLS_POD radosgw-admin bucket stats --bucket=$bucket
# Delete bucket: oc rsh -n openshift-storage \$ROOK_TOOLS_POD radosgw-admin bucket rm --bucket=$bucket --purge-objects
EOF
    done
    [[ $(jq '.orphans.rgw | length' "$JSON_REPORT") -eq 0 ]] && echo "# No RGW bucket data available" >> "$CLEANUP_SCRIPT"
    echo "" >> "$CLEANUP_SCRIPT"
else
    if [[ -f "$AUDIT_DIR/rgw-buckets-list.txt" ]] && [[ -f "$AUDIT_DIR/obc-bucket-names.txt" ]]; then
        BUCKETS=$(cat "$AUDIT_DIR/rgw-buckets-list.txt" | jq -r '.[]' 2>/dev/null || echo "")
        for bucket in $BUCKETS; do
            if ! grep -q "^${bucket}$" "$AUDIT_DIR/obc-bucket-names.txt" 2>/dev/null; then
                cat <<EOF >> "$CLEANUP_SCRIPT"

# Bucket: $bucket
EOF
                if [[ -f "$AUDIT_DIR/rgw-bucket-stats-${bucket}.txt" ]]; then
                    owner=$(cat "$AUDIT_DIR/rgw-bucket-stats-${bucket}.txt" | jq -r '.owner' 2>/dev/null || echo "unknown")
                    objects=$(cat "$AUDIT_DIR/rgw-bucket-stats-${bucket}.txt" | jq -r '.usage["rgw.main"].num_objects' 2>/dev/null || echo "unknown")
                    size=$(cat "$AUDIT_DIR/rgw-bucket-stats-${bucket}.txt" | jq -r '.usage["rgw.main"].size_kb_actual' 2>/dev/null || echo "unknown")
                    echo "# Owner: $owner | Objects: $objects | Size: ${size}KB" >> "$CLEANUP_SCRIPT"
                fi
                cat <<EOF >> "$CLEANUP_SCRIPT"
# Verify bucket contents: oc rsh -n openshift-storage \$ROOK_TOOLS_POD radosgw-admin bucket stats --bucket=$bucket
# Delete bucket: oc rsh -n openshift-storage \$ROOK_TOOLS_POD radosgw-admin bucket rm --bucket=$bucket --purge-objects
EOF
            fi
        done
    else
        echo "# No RGW bucket data available" >> "$CLEANUP_SCRIPT"
    fi
    echo "" >> "$CLEANUP_SCRIPT"
fi

# =============================================================================
# Loki-specific analysis
# =============================================================================
cat <<'EOF' >> "$CLEANUP_SCRIPT"
# =============================================================================
# LOKI-RELATED RESOURCES
# =============================================================================

EOF

if [[ "$JSON_AVAILABLE" == true ]]; then
    if [[ $(jq '.orphans.loki_buckets | length' "$JSON_REPORT") -gt 0 ]]; then
        echo "# Loki-related buckets detected:" >> "$CLEANUP_SCRIPT"
        jq -r '.orphans.loki_buckets[]?.name' "$JSON_REPORT" | sed 's/^/#   - /' >> "$CLEANUP_SCRIPT"
    else
        echo "# No Loki-related resources detected" >> "$CLEANUP_SCRIPT"
    fi
else
    if grep -q "loki" "$AUDIT_DIR/report.txt" 2>/dev/null; then
        echo "# Found Loki-related resources - review section 10.3 of main report" >> "$CLEANUP_SCRIPT"
        grep -A 10 "10.3. Loki-Related Storage Check" "$AUDIT_DIR/report.txt" | grep -E "bucket|PVC" >> "$CLEANUP_SCRIPT" 2>/dev/null || true
    else
        echo "# No Loki-related resources detected" >> "$CLEANUP_SCRIPT"
    fi
fi

# =============================================================================
# Manual cleanup verification commands
# =============================================================================
cat <<'EOF' >> "$CLEANUP_SCRIPT"

# =============================================================================
# VERIFICATION COMMANDS
# Run these before any deletion to verify resource status
# =============================================================================

# Check cluster status
# oc rsh -n openshift-storage $ROOK_TOOLS_POD ceph -s

# Check cluster health
# oc rsh -n openshift-storage $ROOK_TOOLS_POD ceph health detail

# List all RBD watchers (active connections)
# oc rsh -n openshift-storage $ROOK_TOOLS_POD rbd status <pool>/<image>

# Verify PV/PVC bindings
# oc get pv,pvc --all-namespaces

EOF

# =============================================================================
# Summary stats
# =============================================================================
cat <<'EOF' >> "$CLEANUP_SCRIPT"
# =============================================================================
# SUMMARY
# =============================================================================

EOF

if [[ "$JSON_AVAILABLE" == true ]]; then
    rbd_orphans=$(jq '.orphans.rbd | length' "$JSON_REPORT")
    rgw_orphans=$(jq '.orphans.rgw | length' "$JSON_REPORT")
    total_buckets=$(jq '.datasets.rgw | length' "$JSON_REPORT")
    cat <<EOF >> "$CLEANUP_SCRIPT"
# Potentially orphaned RBD images: $rbd_orphans
# Total RGW buckets: $total_buckets
# Potentially orphaned buckets: $rgw_orphans
EOF
else
    orphaned_rbd=$(grep -c "ORPHANED?: .*/" "$AUDIT_DIR/report.txt" 2>/dev/null || echo "0")
    echo "# Potentially orphaned RBD images: $orphaned_rbd" >> "$CLEANUP_SCRIPT"
    if [[ -f "$AUDIT_DIR/rgw-buckets-list.txt" ]] && [[ -f "$AUDIT_DIR/obc-bucket-names.txt" ]]; then
        total_buckets=$(cat "$AUDIT_DIR/rgw-buckets-list.txt" | jq -r '.[] | length' 2>/dev/null | wc -l || echo "0")
        obc_buckets=$(wc -l < "$AUDIT_DIR/obc-bucket-names.txt" 2>/dev/null || echo "0")
        orphaned_buckets=$((total_buckets - obc_buckets))
        cat <<EOF >> "$CLEANUP_SCRIPT"
# Total RGW buckets: $total_buckets
# Buckets with OBC: $obc_buckets
# Potentially orphaned buckets: $orphaned_buckets
EOF
    fi
fi

cat <<'EOF' >> "$CLEANUP_SCRIPT"

# Review this file carefully before executing any commands!
# Uncomment commands you want to execute after verification
EOF

chmod +x "$CLEANUP_SCRIPT"

echo "====================================="
echo "Cleanup commands generated!"
echo "====================================="
echo ""
echo "File: $CLEANUP_SCRIPT"
echo ""
echo "Next steps:"
echo "1. Review the main audit report: $AUDIT_DIR/report.txt"
echo "2. Review the cleanup script: $CLEANUP_SCRIPT"
echo "3. Verify each resource before deletion"
echo "4. Uncomment and execute commands individually"
echo ""
echo "⚠️  All deletion commands are commented out for safety"
echo "⚠️  Review and uncomment only after verification"
