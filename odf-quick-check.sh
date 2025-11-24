#!/bin/bash
#
# ODF/Ceph Quick Health Check
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

require_commands oc jq bc
ensure_rook_tools_pod

rook_exec() {
    oc rsh -n openshift-storage "$ROOK_TOOLS_POD" "$@" 2>&1
}

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo "╔════════════════════════════════════════════════════════════╗"
echo "║       ODF/Ceph Quick Health Check - $(date +%Y-%m-%d)           ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# =============================================================================
# 1. CLUSTER HEALTH
# =============================================================================
echo "📊 Cluster Health:"
HEALTH=$(rook_exec ceph health | head -1)

if [[ "$HEALTH" == "HEALTH_OK" ]]; then
    echo -e "  ${GREEN}✓${NC} $HEALTH"
elif [[ "$HEALTH" == "HEALTH_WARN" ]]; then
    echo -e "  ${YELLOW}⚠${NC} $HEALTH"
    rook_exec ceph health detail | head -5 | sed 's/^/    /'
else
    echo -e "  ${RED}✗${NC} $HEALTH"
    rook_exec ceph health detail | head -10 | sed 's/^/    /'
fi
echo ""

# =============================================================================
# 2. CLUSTER CAPACITY
# =============================================================================
echo "💾 Storage Capacity:"
CAPACITY=$(rook_exec ceph df | grep -A 1 "RAW STORAGE" | tail -1)
USED_RAW=$(echo "$CAPACITY" | awk '{print $3" "$4}')
AVAIL_RAW=$(echo "$CAPACITY" | awk '{print $5" "$6}')
PERCENT=$(echo "$CAPACITY" | awk '{print $7}' | sed 's/%//')

echo "  Used: $USED_RAW | Available: $AVAIL_RAW | Usage: ${PERCENT}%"

if [[ $(echo "$PERCENT > 85" | bc -l) -eq 1 ]]; then
    echo -e "  ${RED}⚠ WARNING: Usage above 85%${NC}"
elif [[ $(echo "$PERCENT > 75" | bc -l) -eq 1 ]]; then
    echo -e "  ${YELLOW}⚠ CAUTION: Usage above 75%${NC}"
else
    echo -e "  ${GREEN}✓${NC} Storage OK"
fi
echo ""

# =============================================================================
# 3. OSD STATUS
# =============================================================================
echo "🔧 OSD Status:"
OSD_STAT=$(rook_exec ceph osd stat)
echo "  $OSD_STAT"

OSD_DOWN=$(rook_exec ceph osd tree | grep -c "down" || echo "0")
if [[ $OSD_DOWN -gt 0 ]]; then
    echo -e "  ${RED}⚠ WARNING: $OSD_DOWN OSDs are down!${NC}"
    rook_exec ceph osd tree | grep "down" | sed 's/^/    /'
else
    echo -e "  ${GREEN}✓${NC} All OSDs up"
fi
echo ""

# =============================================================================
# 4. PG STATUS
# =============================================================================
echo "📋 Placement Group Status:"
PG_STAT=$(rook_exec ceph pg stat)
echo "  $PG_STAT"

PG_INACTIVE=$(rook_exec ceph pg stat | grep -o "active" | wc -l)
if [[ $PG_INACTIVE -eq 0 ]]; then
    echo -e "  ${RED}⚠ WARNING: Some PGs are not active+clean${NC}"
    rook_exec ceph pg dump pgs 2>/dev/null | grep -v "active+clean" | head -5 | sed 's/^/    /' || echo "    Check manually: oc rsh -n openshift-storage $ROOK_TOOLS_POD ceph pg dump"
else
    echo -e "  ${GREEN}✓${NC} PGs healthy"
fi
echo ""

# =============================================================================
# 5. POOL USAGE TOP 5
# =============================================================================
echo "📦 Top 5 Pools by Usage:"
rook_exec rados df 2>/dev/null | grep -v "^POOL_NAME" | grep -v "^---" | grep -v "^total_" | \
    sort -k3 -h -r | head -5 | while read -r line; do
        echo "  $line"
    done
echo ""

# =============================================================================
# 6. RECENT EVENTS/WARNINGS
# =============================================================================
echo "⚡ Recent Ceph Events (last 10):"
rook_exec ceph -W cephadm --watch-debug=0 2>/dev/null | head -10 | sed 's/^/  /' || \
    echo "  (Event log unavailable - check manually)"
echo ""

# =============================================================================
# 7. KUBERNETES RESOURCES
# =============================================================================
echo "☸️  Kubernetes Storage Resources:"

# PV count by storage class
PV_RBD=$(oc get pv -o json | jq -r '.items[] | select(.spec.csi.driver=="openshift-storage.rbd.csi.ceph.com")' | jq -s 'length' 2>/dev/null || echo "0")
PV_CEPHFS=$(oc get pv -o json | jq -r '.items[] | select(.spec.csi.driver=="openshift-storage.cephfs.csi.ceph.com")' | jq -s 'length' 2>/dev/null || echo "0")
PV_PENDING=$(oc get pv 2>/dev/null | grep -c "Pending" || echo "0")
PV_FAILED=$(oc get pv 2>/dev/null | grep -c "Failed" || echo "0")

echo "  PVs - RBD: $PV_RBD | CephFS: $PV_CEPHFS"

if [[ $PV_PENDING -gt 0 ]]; then
    echo -e "  ${YELLOW}⚠${NC} Pending PVs: $PV_PENDING"
fi

if [[ $PV_FAILED -gt 0 ]]; then
    echo -e "  ${RED}⚠ WARNING: Failed PVs: $PV_FAILED${NC}"
    oc get pv | grep "Failed" | head -5 | sed 's/^/    /'
fi

# OBC count
OBC_COUNT=$(oc get objectbucketclaim --all-namespaces --no-headers 2>/dev/null | awk 'END{print NR}' || echo "0")
echo "  ObjectBucketClaims: $OBC_COUNT"
echo ""

# =============================================================================
# 8. OPERATOR STATUS
# =============================================================================
echo "🔄 Operator Status:"
OCS_OPERATOR=$(oc get pods -n openshift-storage -l name=ocs-operator -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "NotFound")
ROOK_OPERATOR=$(oc get pods -n openshift-storage -l app=rook-ceph-operator -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "NotFound")
NOOBAA_OPERATOR=$(oc get pods -n openshift-storage -l app=noobaa -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "NotFound")

echo "  OCS Operator: $OCS_OPERATOR"
echo "  Rook Operator: $ROOK_OPERATOR"
echo "  NooBaa Operator: $NOOBAA_OPERATOR"

if [[ "$OCS_OPERATOR" != "Running" ]] || [[ "$ROOK_OPERATOR" != "Running" ]]; then
    echo -e "  ${RED}⚠ WARNING: Some operators are not running!${NC}"
fi
echo ""

# =============================================================================
# 9. QUICK ORPHANED CHECK
# =============================================================================
echo "🔍 Quick Orphaned Resource Check:"

# Count RBD pools and images
RBD_POOLS=$(rook_exec ceph osd pool ls | grep -Ec 'rbd|block' || echo "0")
if [[ $RBD_POOLS -gt 0 ]]; then
    TOTAL_RBD_IMAGES=0
    for pool in $(rook_exec ceph osd pool ls | grep -E 'rbd|block'); do
        COUNT=$(rook_exec rbd ls "$pool" 2>/dev/null | wc -l || echo "0")
        TOTAL_RBD_IMAGES=$((TOTAL_RBD_IMAGES + COUNT))
    done
    echo "  Total RBD images in cluster: $TOTAL_RBD_IMAGES"
    echo "  RBD PVs in Kubernetes: $PV_RBD"
    DIFF=$((TOTAL_RBD_IMAGES - PV_RBD))
    if [[ $DIFF -gt 5 ]]; then
        echo -e "  ${YELLOW}⚠ Potential orphaned RBD images: $DIFF${NC}"
        echo "    Run full audit for details: ./odf-audit.sh"
    fi
fi

# Count RGW buckets
RGW_BUCKETS=$(rook_exec radosgw-admin bucket list 2>/dev/null | jq -r '.[]' 2>/dev/null | wc -l || echo "0")
if [[ $RGW_BUCKETS -gt 0 ]]; then
    echo "  Total RGW buckets: $RGW_BUCKETS"
    echo "  ObjectBucketClaims: $OBC_COUNT"
    DIFF=$((RGW_BUCKETS - OBC_COUNT))
    if [[ $DIFF -gt 0 ]]; then
        echo -e "  ${YELLOW}⚠ Potential orphaned buckets: $DIFF${NC}"
        echo "    Run full audit for details: ./odf-audit.sh"
    fi
fi
echo ""

# =============================================================================
# 10. RECOMMENDATIONS
# =============================================================================
echo "💡 Recommendations:"

# Storage threshold check
if [[ $(echo "$PERCENT > 85" | bc -l) -eq 1 ]]; then
    echo -e "  ${RED}• URGENT: Free up space or add capacity${NC}"
    echo "    Run: ./odf-top-consumers.sh to find largest consumers"
elif [[ $(echo "$PERCENT > 75" | bc -l) -eq 1 ]]; then
    echo -e "  ${YELLOW}• Consider planning capacity expansion${NC}"
    echo "    Run: ./odf-top-consumers.sh to analyze usage"
fi

# Health check
if [[ "$HEALTH" != "HEALTH_OK" ]]; then
    echo -e "  ${YELLOW}• Investigate cluster health warnings${NC}"
    echo "    Run: oc rsh -n openshift-storage $ROOK_TOOLS_POD ceph health detail"
fi

# Orphaned resources
if [[ $DIFF -gt 5 ]] 2>/dev/null; then
    echo -e "  ${YELLOW}• Run full audit to identify orphaned resources${NC}"
    echo "    Run: ./odf-audit.sh"
fi

# If all OK
if [[ "$HEALTH" == "HEALTH_OK" ]] && [[ $(echo "$PERCENT < 75" | bc -l) -eq 1 ]] && [[ $PV_FAILED -eq 0 ]]; then
    echo -e "  ${GREEN}✓ No immediate actions required${NC}"
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "Quick check complete. For detailed analysis run: ./odf-audit.sh"
echo "════════════════════════════════════════════════════════════"
