#!/bin/bash
#
# ODF/Ceph Interactive Resource Inspector
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

require_commands oc jq
ensure_rook_tools_pod

rook_exec() {
    oc rsh -n openshift-storage "$ROOK_TOOLS_POD" "$@" 2>&1
}

# Colors
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

show_menu() {
    clear
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║     ODF/Ceph Interactive Resource Inspector                ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Select inspection type:"
    echo ""
    echo "  1) Inspect RBD image"
    echo "  2) Inspect RGW bucket"
    echo "  3) Inspect PV (Persistent Volume)"
    echo "  4) Inspect PVC (Persistent Volume Claim)"
    echo "  5) Inspect pool"
    echo "  6) Find PV by namespace/PVC name"
    echo "  7) List all resources of type"
    echo "  8) Search resources by pattern"
    echo ""
    echo "  0) Exit"
    echo ""
    echo -n "Enter choice [0-8]: "
}

inspect_rbd_image() {
    echo ""
    echo "═══ RBD Image Inspector ═══"
    echo ""
    
    # List pools
    echo "Available RBD pools:"
    rook_exec ceph osd pool ls | grep -E 'rbd|block' | nl -v 1
    echo ""
    echo -n "Enter pool name: "
    read -r pool
    
    if [[ -z "$pool" ]]; then
        echo -e "${RED}Pool name required${NC}"
        return
    fi
    
    # List images in pool
    echo ""
    echo "Images in pool '$pool':"
    rook_exec rbd ls "$pool" 2>/dev/null | nl -v 1 || echo "No images or pool not found"
    echo ""
    echo -n "Enter image name: "
    read -r image
    
    if [[ -z "$image" ]]; then
        echo -e "${RED}Image name required${NC}"
        return
    fi
    
    echo ""
    echo -e "${BLUE}═══ Detailed Info for $pool/$image ═══${NC}"
    echo ""
    
    # Basic info
    echo "📋 Basic Information:"
    rook_exec rbd info "$pool/$image" 2>&1
    echo ""
    
    # Check if in use (watchers)
    echo "👁️  Watchers (who is using this image):"
    WATCHERS=$(rook_exec rbd status "$pool/$image" 2>&1)
    echo "$WATCHERS"
    
    if echo "$WATCHERS" | grep -qi "watcher"; then
        echo -e "${YELLOW}⚠ Image is IN USE${NC}"
    else
        echo -e "${GREEN}✓ Image is NOT in use${NC}"
    fi
    echo ""
    
    # Check if has corresponding PV
    echo "☸️  Kubernetes Integration:"
    PV_MATCH=$(oc get pv -o json | jq -r --arg img "$image" '.items[] | select(.spec.csi.driver=="openshift-storage.rbd.csi.ceph.com") | select(.spec.csi.volumeHandle | contains($img)) | .metadata.name' 2>/dev/null || echo "")
    
    if [[ -n "$PV_MATCH" ]]; then
        echo -e "${GREEN}✓ Found matching PV: $PV_MATCH${NC}"
        PVC=$(oc get pv "$PV_MATCH" -o jsonpath='{.spec.claimRef.namespace}/{.spec.claimRef.name}' 2>/dev/null || echo "")
        if [[ -n "$PVC" ]]; then
            echo "  Claimed by PVC: $PVC"
            
            # Check if PVC is used by pod
            NAMESPACE=$(echo "$PVC" | cut -d'/' -f1)
            PVC_NAME=$(echo "$PVC" | cut -d'/' -f2)
            PODS=$(oc get pods -n "$NAMESPACE" -o json 2>/dev/null | \
                jq -r --arg pvc "$PVC_NAME" '.items[] | select(.spec.volumes[]?.persistentVolumeClaim.claimName == $pvc) | .metadata.name' || echo "")
            
            if [[ -n "$PODS" ]]; then
                echo "  Used by pods:"
                while IFS= read -r pod; do
                    printf '    - %s\n' "$pod"
                done <<< "$PODS"
            else
                echo -e "  ${YELLOW}⚠ PVC exists but not used by any pod${NC}"
            fi
        fi
    else
        echo -e "${YELLOW}⚠ NO matching PV found (potentially orphaned)${NC}"
    fi
    echo ""
    
    # Snapshots
    echo "📸 Snapshots:"
    rook_exec rbd snap ls "$pool/$image" 2>&1 | head -20 || echo "No snapshots"
    echo ""
    
    echo -e "${BLUE}═══ Deletion Safety Check ═══${NC}"
    if echo "$WATCHERS" | grep -qi "watcher"; then
        echo -e "${RED}⚠ NOT SAFE TO DELETE - Image is in use${NC}"
    elif [[ -n "$PV_MATCH" ]]; then
        echo -e "${YELLOW}⚠ CAUTION - Has corresponding PV/PVC${NC}"
        echo "  Remove PVC first: oc delete pvc -n $NAMESPACE $PVC_NAME"
    else
        echo -e "${GREEN}✓ Appears safe to delete (verify no application dependencies)${NC}"
        echo "  Delete command: oc rsh -n openshift-storage $ROOK_TOOLS_POD rbd rm $pool/$image"
    fi
    echo ""
}

inspect_rgw_bucket() {
    echo ""
    echo "═══ RGW Bucket Inspector ═══"
    echo ""
    
    # List all buckets
    echo "Available buckets:"
    rook_exec radosgw-admin bucket list 2>&1 | jq -r '.[]' 2>/dev/null | nl -v 1 || echo "No buckets found"
    echo ""
    echo -n "Enter bucket name: "
    read -r bucket
    
    if [[ -z "$bucket" ]]; then
        echo -e "${RED}Bucket name required${NC}"
        return
    fi
    
    echo ""
    echo -e "${BLUE}═══ Detailed Info for bucket: $bucket ═══${NC}"
    echo ""
    
    # Bucket stats
    echo "📊 Bucket Statistics:"
    STATS=$(rook_exec radosgw-admin bucket stats --bucket="$bucket" 2>&1)
    echo "$STATS" | jq '.' 2>/dev/null || echo "$STATS"
    echo ""
    
    # Extract key info
    OWNER=$(echo "$STATS" | jq -r '.owner' 2>/dev/null || echo "unknown")
    OBJECTS=$(echo "$STATS" | jq -r '.usage["rgw.main"].num_objects' 2>/dev/null || echo "0")
    SIZE_KB=$(echo "$STATS" | jq -r '.usage["rgw.main"].size_kb_actual' 2>/dev/null || echo "0")
    
    echo "Owner: $OWNER"
    echo "Objects: $OBJECTS"
    echo "Size: $SIZE_KB KB"
    echo ""
    
    # Check for OBC
    echo "☸️  Kubernetes Integration:"
    OBC_MATCH=$(oc get objectbucketclaim --all-namespaces -o json 2>/dev/null | \
        jq -r --arg bucket "$bucket" '.items[] | select(.spec.bucketName == $bucket) | "\(.metadata.namespace)/\(.metadata.name)"' || echo "")
    
    if [[ -n "$OBC_MATCH" ]]; then
        echo -e "${GREEN}✓ Found matching ObjectBucketClaim: $OBC_MATCH${NC}"
    else
        echo -e "${YELLOW}⚠ NO matching ObjectBucketClaim (potentially orphaned or manually created)${NC}"
    fi
    echo ""
    
    # List objects (sample)
    echo "📦 Objects (first 20):"
    rook_exec radosgw-admin bucket list --bucket="$bucket" 2>&1 | jq -r '.[] | .name' 2>/dev/null | head -20 || echo "Unable to list objects"
    echo ""
    
    # Check bucket policy
    echo "🔒 Bucket Policy:"
    rook_exec radosgw-admin bucket policy --bucket="$bucket" 2>&1 | head -20 || echo "No policy set"
    echo ""
    
    echo -e "${BLUE}═══ Deletion Safety Check ═══${NC}"
    if [[ -n "$OBC_MATCH" ]]; then
        echo -e "${YELLOW}⚠ CAUTION - Has corresponding ObjectBucketClaim${NC}"
        NAMESPACE=$(echo "$OBC_MATCH" | cut -d'/' -f1)
        OBC_NAME=$(echo "$OBC_MATCH" | cut -d'/' -f2)
        echo "  Remove OBC first: oc delete objectbucketclaim -n $NAMESPACE $OBC_NAME"
    else
        echo -e "${YELLOW}⚠ Review contents before deletion${NC}"
        echo "  Objects in bucket: $OBJECTS"
        echo "  Delete command: oc rsh -n openshift-storage $ROOK_TOOLS_POD radosgw-admin bucket rm --bucket=$bucket --purge-objects"
    fi
    echo ""
}

inspect_pv() {
    echo ""
    echo "═══ PV Inspector ═══"
    echo ""
    
    echo "Available PVs (first 30):"
    oc get pv -o wide | head -31
    echo ""
    echo -n "Enter PV name: "
    read -r pv
    
    if [[ -z "$pv" ]]; then
        echo -e "${RED}PV name required${NC}"
        return
    fi
    
    echo ""
    echo -e "${BLUE}═══ Detailed Info for PV: $pv ═══${NC}"
    echo ""
    
    # Get PV details
    PV_JSON=$(oc get pv "$pv" -o json 2>&1)
    
    if echo "$PV_JSON" | grep -q "NotFound"; then
        echo -e "${RED}PV not found: $pv${NC}"
        return
    fi
    
    echo "📋 Basic Info:"
    oc get pv "$pv" -o wide
    echo ""
    
    # Extract details
    DRIVER=$(echo "$PV_JSON" | jq -r '.spec.csi.driver' 2>/dev/null || echo "unknown")
    VOLUME_HANDLE=$(echo "$PV_JSON" | jq -r '.spec.csi.volumeHandle' 2>/dev/null || echo "unknown")
    PVC_NS=$(echo "$PV_JSON" | jq -r '.spec.claimRef.namespace' 2>/dev/null || echo "")
    PVC_NAME=$(echo "$PV_JSON" | jq -r '.spec.claimRef.name' 2>/dev/null || echo "")
    
    echo "Driver: $DRIVER"
    echo "Volume Handle: $VOLUME_HANDLE"
    echo ""
    
    if [[ -n "$PVC_NS" ]] && [[ -n "$PVC_NAME" ]]; then
        echo "☸️  Claimed by:"
        echo "  Namespace: $PVC_NS"
        echo "  PVC: $PVC_NAME"
        echo ""
        
        # Check if PVC is used
        PODS=$(oc get pods -n "$PVC_NS" -o json 2>/dev/null | \
            jq -r --arg pvc "$PVC_NAME" '.items[] | select(.spec.volumes[]?.persistentVolumeClaim.claimName == $pvc) | .metadata.name' || echo "")
        
        if [[ -n "$PODS" ]]; then
            echo "  Used by pods:"
            while IFS= read -r pod; do
                printf '    - %s\n' "$pod"
            done <<< "$PODS"
        else
            echo -e "  ${YELLOW}⚠ Not used by any pod${NC}"
        fi
    else
        echo -e "${YELLOW}⚠ PV is not claimed (Available/Released)${NC}"
    fi
    echo ""
    
    # If RBD, show underlying image
    if [[ "$DRIVER" == *"rbd"* ]]; then
        echo "💾 RBD Backend:"
        IMAGE_ID=$(echo "$VOLUME_HANDLE" | awk -F'-' '{print $NF}')
        echo "  Image ID: $IMAGE_ID"
        
        # Try to find the image
        for pool in $(rook_exec ceph osd pool ls | grep -E 'rbd|block'); do
            if rook_exec rbd ls "$pool" 2>/dev/null | grep -q "$IMAGE_ID"; then
                echo "  Found in pool: $pool"
                rook_exec rbd info "$pool/$IMAGE_ID" 2>&1 | head -10
                break
            fi
        done
    fi
    echo ""
}

find_pv_by_pvc() {
    echo ""
    echo "═══ Find PV by PVC ═══"
    echo ""
    
    echo -n "Enter namespace: "
    read -r namespace
    echo -n "Enter PVC name: "
    read -r pvc_name
    
    if [[ -z "$namespace" ]] || [[ -z "$pvc_name" ]]; then
        echo -e "${RED}Both namespace and PVC name required${NC}"
        return
    fi
    
    echo ""
    echo "Searching for PVC: $namespace/$pvc_name"
    echo ""
    
    PVC_INFO=$(oc get pvc -n "$namespace" "$pvc_name" -o json 2>&1)
    
    if echo "$PVC_INFO" | grep -q "NotFound"; then
        echo -e "${RED}PVC not found: $namespace/$pvc_name${NC}"
        return
    fi
    
    PV_NAME=$(echo "$PVC_INFO" | jq -r '.spec.volumeName' 2>/dev/null || echo "")
    
    if [[ -z "$PV_NAME" ]] || [[ "$PV_NAME" == "null" ]]; then
        echo -e "${YELLOW}⚠ PVC exists but not bound to any PV${NC}"
        echo "$PVC_INFO" | jq -r '.status'
        return
    fi
    
    echo -e "${GREEN}✓ Found PV: $PV_NAME${NC}"
    echo ""
    
    # Show full details
    inspect_pv_internal "$PV_NAME"
}

list_resources() {
    echo ""
    echo "═══ List Resources ═══"
    echo ""
    echo "1) RBD images (all pools)"
    echo "2) RGW buckets"
    echo "3) Persistent Volumes"
    echo "4) ObjectBucketClaims"
    echo "5) Ceph pools"
    echo ""
    echo -n "Enter choice [1-5]: "
    read -r choice
    
    case $choice in
        1)
            echo ""
            echo "RBD Images by pool:"
            for pool in $(rook_exec ceph osd pool ls | grep -E 'rbd|block'); do
                echo ""
                echo "Pool: $pool"
                rook_exec rbd ls "$pool" 2>/dev/null | sed 's/^/  - /' || echo "  No images"
            done
            ;;
        2)
            echo ""
            echo "RGW Buckets:"
            rook_exec radosgw-admin bucket list 2>&1 | jq -r '.[]' 2>/dev/null | sed 's/^/  - /' || echo "No buckets"
            ;;
        3)
            echo ""
            oc get pv -o wide
            ;;
        4)
            echo ""
            oc get objectbucketclaim --all-namespaces -o wide
            ;;
        5)
            echo ""
            echo "Ceph Pools:"
            rook_exec ceph osd pool ls detail
            ;;
        *)
            echo "Invalid choice"
            ;;
    esac
    echo ""
}

# Main loop
while true; do
    show_menu
    read -r choice
    
    case $choice in
        1) inspect_rbd_image ;;
        2) inspect_rgw_bucket ;;
        3) 
            echo ""
            echo -n "Enter PV name: "
            read -r pv
            inspect_pv
            ;;
        4) 
            echo ""
            echo "PVC Inspector - redirecting to find PV..."
            find_pv_by_pvc
            ;;
        5)
            echo ""
            echo -n "Enter pool name: "
            read -r pool
            echo ""
            rook_exec ceph osd pool stats "$pool" 2>&1
            echo ""
            rook_exec rados df | grep -A 1 "$pool"
            ;;
        6) find_pv_by_pvc ;;
        7) list_resources ;;
        8)
            echo ""
            echo -n "Enter search pattern: "
            read -r pattern
            echo ""
            echo "Searching PVs matching '$pattern':"
            oc get pv | grep -i "$pattern" || echo "No matches"
            echo ""
            echo "Searching PVCs matching '$pattern':"
            oc get pvc --all-namespaces | grep -i "$pattern" || echo "No matches"
            ;;
        0) 
            echo "Exiting..."
            exit 0
            ;;
        *)
            echo "Invalid choice"
            ;;
    esac
    
    echo ""
    echo -n "Press Enter to continue..."
    read -r
done
