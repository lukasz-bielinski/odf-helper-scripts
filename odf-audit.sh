#!/bin/bash
#
# Comprehensive ODF/Ceph storage audit.
# Generates a structured report, JSON data set, and cleanup hints
# covering Ceph RBD/FS/RGW plus Kubernetes PV/PVC/OBC mappings.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

require_commands oc jq bc awk sed grep column

AUDIT_DIR="${1:-${ODF_AUDIT_DIR:-/tmp/odf-audit-$(date +%Y%m%d-%H%M%S)}}"
DATA_DIR="$AUDIT_DIR/data"
mkdir -p "$DATA_DIR"

REPORT_FILE="$AUDIT_DIR/report.txt"
SUMMARY_FILE="$AUDIT_DIR/SUMMARY.txt"
JSON_FILE="$AUDIT_DIR/report.json"
RUN_LOG="$AUDIT_DIR/audit.log"
POOL_STATS_JSON="$DATA_DIR/ceph-df-detail.json"

: > "$REPORT_FILE"
: > "$RUN_LOG"

section() {
    printf "\n=== %s ===\n" "$1" | tee -a "$REPORT_FILE"
}

subsection() {
    printf "\n--- %s ---\n" "$1" | tee -a "$REPORT_FILE"
}

append_line() {
    printf "%s\n" "$1" | tee -a "$REPORT_FILE"
}

log_line() {
    printf "[%s] %s\n" "$(timestamp)" "$1" | tee -a "$RUN_LOG"
}

write_json_file() {
    local path=$1
    local content=$2
    printf '%s\n' "$content" > "$path"
}

pv_volume_map="$DATA_DIR/pv-volume-handles.txt"
obc_bucket_map="$DATA_DIR/obc-bucket-names.txt"
rbd_ndjson="$DATA_DIR/rbd-images.ndjson"
rgw_ndjson="$DATA_DIR/rgw-buckets.ndjson"
cephfs_ndjson="$DATA_DIR/cephfs-subvols.ndjson"

: > "$pv_volume_map"
: > "$obc_bucket_map"
: > "$rbd_ndjson"
: > "$rgw_ndjson"
: > "$cephfs_ndjson"

init_header() {
    section "ODF / Ceph Storage Audit"
    append_line "Generated: $(timestamp)"
    ensure_rook_tools_pod
    append_line "rook-ceph-tools pod: $ROOK_TOOLS_POD"
    append_line "Output directory: $AUDIT_DIR"
}

collect_k8s_data() {
    log_line "Collecting Kubernetes resources"
    oc get pv -o json > "$DATA_DIR/pv.json"
    oc get pvc --all-namespaces -o json > "$DATA_DIR/pvc.json"
    if ! oc get objectbucketclaim --all-namespaces -o json > "$DATA_DIR/obc.json"; then
        echo '{"items":[]}' > "$DATA_DIR/obc.json"
    fi
    if ! oc get objectbucket -o json > "$DATA_DIR/ob.json"; then
        echo '{"items":[]}' > "$DATA_DIR/ob.json"
    fi

    jq -r '.items[] | select(.spec.csi.driver=="openshift-storage.rbd.csi.ceph.com") | .spec.csi.volumeHandle // ""' "$DATA_DIR/pv.json" \
        | sed '/^$/d' > "$pv_volume_map"

    jq -r '.items[] | .spec.bucketName // ""' "$DATA_DIR/obc.json" \
        | sed '/^$/d' > "$obc_bucket_map"

    section "Kubernetes Storage Inventory"
    local pv_total pvc_total obc_total
    pv_total=$(jq '.items | length' "$DATA_DIR/pv.json")
    pvc_total=$(jq '.items | length' "$DATA_DIR/pvc.json")
    obc_total=$(jq '.items | length' "$DATA_DIR/obc.json")

    append_line "PVs total: $pv_total (RBD: $(jq '.items[] | select(.spec.csi.driver=="openshift-storage.rbd.csi.ceph.com") | 1' "$DATA_DIR/pv.json" | wc -l), CephFS: $(jq '.items[] | select(.spec.csi.driver=="openshift-storage.cephfs.csi.ceph.com") | 1' "$DATA_DIR/pv.json" | wc -l))"
    append_line "PVCs total: $pvc_total"
    append_line "ObjectBucketClaims total: $obc_total"
}

collect_ceph_cluster() {
    log_line "Collecting Ceph metrics from Prometheus"
    
    # Collect ceph status for health info (no Prometheus equivalent for all fields)
    rook_exec ceph status --format json > "$DATA_DIR/ceph-status.json"
    
    # Collect Prometheus metrics - REQUIRED, no fallback
    query_prometheus "sum(ceph_osd_stat_bytes)" > "$DATA_DIR/prom-cluster-total.json"
    query_prometheus "sum(ceph_pool_bytes_used)" > "$DATA_DIR/prom-cluster-used.json"
    query_prometheus "sum(ceph_pool_stored)" > "$DATA_DIR/prom-cluster-stored.json"
    query_prometheus "ceph_pool_bytes_used" > "$DATA_DIR/prom-pool-bytes-used.json"
    query_prometheus "ceph_pool_stored" > "$DATA_DIR/prom-pool-stored.json"
    query_prometheus "ceph_pool_metadata" > "$DATA_DIR/prom-pool-metadata.json"

    section "Ceph Cluster Status"
    local health osds mons total_bytes used_bytes stored_bytes
    health=$(jq -r '.health.status // "unknown"' "$DATA_DIR/ceph-status.json")
    osds=$(jq '.osdmap.num_osds // 0' "$DATA_DIR/ceph-status.json")
    mons=$(jq '.monmap.mons | length // 0' "$DATA_DIR/ceph-status.json")
    
    # Extract capacity from Prometheus metrics
    total_bytes=$(extract_metric_value "$(cat "$DATA_DIR/prom-cluster-total.json")")
    used_bytes=$(extract_metric_value "$(cat "$DATA_DIR/prom-cluster-used.json")")
    stored_bytes=$(extract_metric_value "$(cat "$DATA_DIR/prom-cluster-stored.json")")
    
    # Calculate efficiency ratio
    local efficiency_ratio="N/A"
    if [[ "$used_bytes" != "0" ]] && [[ "$stored_bytes" != "0" ]] && [[ -n "$used_bytes" ]]; then
        efficiency_ratio=$(echo "scale=2; $stored_bytes / $used_bytes" | bc -l)
    fi

    append_line "Health: $health"
    append_line "MONs: $mons | OSDs: $osds"
    append_line "Raw Capacity: $(human_bytes "$total_bytes")"
    append_line "Used (with replication): $(human_bytes "$used_bytes")"
    append_line "Stored (client data): $(human_bytes "$stored_bytes")"
    append_line "Storage Efficiency: ${efficiency_ratio}x"

    subsection "Top pools by usage"
    # Parse Prometheus pool metrics (always in bytes, no guessing!)
    # Debug: check if file exists and is valid JSON
    if [[ ! -f "$DATA_DIR/prom-pool-bytes-used.json" ]]; then
        append_line "  ERROR: prom-pool-bytes-used.json not found"
        return
    fi
    
    if ! jq -e . "$DATA_DIR/prom-pool-bytes-used.json" >/dev/null 2>&1; then
        append_line "  ERROR: prom-pool-bytes-used.json is not valid JSON"
        append_line "  Content preview:"
        head -5 "$DATA_DIR/prom-pool-bytes-used.json" | sed 's/^/    /' | tee -a "$REPORT_FILE"
        return
    fi
    
    # Check if we have results
    local result_count
    result_count=$(jq '.data.result | length' "$DATA_DIR/prom-pool-bytes-used.json" 2>/dev/null || echo "0")
    if [[ "$result_count" == "0" ]]; then
        append_line "  No pool data in Prometheus response"
        append_line "  Response structure:"
        jq '.' "$DATA_DIR/prom-pool-bytes-used.json" | head -10 | sed 's/^/    /' | tee -a "$REPORT_FILE"
        return
    fi
    
    # Ceph Prometheus metrics use pool_id label (numeric ID, not name)
    # We need to get pool metadata to map pool_id to pool name
    local pool_metadata_available=false
    if [[ -f "$DATA_DIR/prom-pool-metadata.json" ]] && jq -e '.data.result[0]' "$DATA_DIR/prom-pool-metadata.json" >/dev/null 2>&1; then
        pool_metadata_available=true
    fi
    
    if [[ "$pool_metadata_available" == true ]]; then
        # Map pool_id to pool name using ceph_pool_metadata
        jq -r --slurpfile metadata "$DATA_DIR/prom-pool-metadata.json" '
            # Build pool_id -> name mapping from metadata
            ($metadata[0].data.result | map({(.metric.pool_id): .metric.name}) | add) as $pool_map |
            .data.result[]? |
            [
                ($pool_map[.metric.pool_id] // ("pool_id_" + .metric.pool_id)),
                (.value[1] | tonumber)
            ] | 
            @tsv
        ' "$DATA_DIR/prom-pool-bytes-used.json" \
            | sort -k2 -n -r | head -10 \
            | awk '{printf "  %-35s %12.2f MiB\n", $1, $2/1024/1024}' \
            | tee -a "$REPORT_FILE"
    else
        # Fallback: show pool_id if metadata unavailable
        jq -r '.data.result[]? | 
            [
                ("pool_id_" + .metric.pool_id),
                (.value[1] | tonumber)
            ] | 
            @tsv' "$DATA_DIR/prom-pool-bytes-used.json" \
            | sort -k2 -n -r | head -10 \
            | awk '{printf "  %-35s %12.2f MiB\n", $1, $2/1024/1024}' \
            | tee -a "$REPORT_FILE"
        append_line "  Note: Pool names unavailable, showing pool_id. Run 'oc get cephblockpools -A' to see names."
    fi
}

collect_rbd_data() {
    section "RBD Pools & Images"
    
    # Get RBD pools from Prometheus metadata
    local pools
    pools=$(jq -r '.data.result[]? | select(.metric.type == "replicated" or .metric.type == "erasure") | .metric.name // .metric.pool_name' \
        "$DATA_DIR/prom-pool-metadata.json" 2>/dev/null | grep -iE 'rbd|block' | sort -u || echo "")
    
    # Fallback to ceph command if Prometheus doesn't have pool data
    if [[ -z "$pools" ]]; then
        pools=$(rook_exec ceph osd pool ls --format json 2>/dev/null | jq -r '.[]' | grep -iE 'rbd|block' || true)
    fi
    if [[ -z "$pools" ]]; then
        pools=$(rook_exec ceph osd pool ls 2>/dev/null | tr -d '[]",' | tr ' ' '\n' | grep -iE 'rbd|block' || true)
    fi

    if [[ -z "$pools" ]]; then
        append_line "No RBD pools detected"
        return
    fi

    local total_images=0
    local orphan_images=0
    local test_pattern_images=0

    for pool in $pools; do
        local list_json
        list_json=$(rook_exec rbd ls "$pool" --format json 2>/dev/null || echo '[]')
        local images
        images=$(echo "$list_json" | jq -r '.[]')
        if [[ -z "$images" ]]; then
            continue
        fi
        subsection "Pool: $pool"
        for image in $images; do
            local info_json status_json size_bytes size_human watcher_count watchers_json has_pv manual_flag is_test_pattern
            info_json=$(rook_exec rbd info "$pool/$image" --format json 2>/dev/null || echo '{}')
            size_bytes=$(echo "$info_json" | jq '.size // 0')
            size_human=$(human_bytes "$size_bytes")
            status_json=$(rook_exec rbd status "$pool/$image" --format json 2>/dev/null || echo '{}')
            watcher_count=$(echo "$status_json" | jq '.watchers | length // 0')
            watchers_json=$(echo "$status_json" | jq -c '.watchers // []')

            # Enhanced PV matching using parse_volume_handle
            has_pv=false
            if [[ -s "$pv_volume_map" ]]; then
                # Try exact image name match (backward compatible)
                if grep -Fq "$image" "$pv_volume_map"; then
                    has_pv=true
                else
                    # Try parsing volumeHandle for pool/image match
                    while IFS= read -r handle; do
                        local parsed
                        parsed=$(parse_volume_handle "$handle")
                        if [[ "$parsed" == "$pool/$image" ]] || [[ "$parsed" == *"$image"* ]]; then
                            has_pv=true
                            break
                        fi
                    done < "$pv_volume_map"
                fi
            fi

            # Check for test/benchmark pattern names
            is_test_pattern=false
            if [[ "$image" =~ (test|bench|benchmark|fio|temp|tmp|demo) ]]; then
                is_test_pattern=true
                ((test_pattern_images++))
            fi

            # Orphan detection
            manual_flag=false
            if [[ "$has_pv" == false && $watcher_count -eq 0 ]]; then
                manual_flag=true
            fi

            jq -n \
                --arg pool "$pool" \
                --arg image "$image" \
                --arg size_human "$size_human" \
                --argjson size_bytes "$size_bytes" \
                --argjson watchers "$watchers_json" \
                --argjson watcher_count "$watcher_count" \
                --argjson has_pv "$has_pv" \
                --argjson manual "$manual_flag" \
                --argjson test_pattern "$is_test_pattern" \
                '{pool:$pool,image:$image,size_bytes:$size_bytes,size_human:$size_human,watchers:$watchers,watcher_count:$watcher_count,has_pv:$has_pv,manual_flag:$manual,test_pattern:$test_pattern}' >> "$rbd_ndjson"

            printf "  %-60s %12s | watchers: %s | bound: %s" "$pool/$image" "$size_human" "$watcher_count" "$has_pv"
            if [[ "$is_test_pattern" == true ]]; then
                printf " | TEST"
            fi
            printf "\n" | tee -a "$REPORT_FILE"

            ((total_images++))
            if [[ "$has_pv" == false ]]; then
                ((orphan_images++))
            fi
        done
    done

    append_line "Total RBD images: $total_images"
    append_line "Potential orphaned images: $orphan_images"
    if [[ $test_pattern_images -gt 0 ]]; then
        append_line "Test/benchmark pattern images: $test_pattern_images (review manually)"
    fi
}

collect_cephfs_data() {
    section "CephFS"
    
    # Collect Prometheus metrics for CephFS
    query_prometheus "ceph_mds_metadata" > "$DATA_DIR/prom-mds-metadata.json" 2>/dev/null || true
    query_prometheus "ceph_mds_root_rbytes" > "$DATA_DIR/prom-mds-rbytes.json" 2>/dev/null || true
    query_prometheus "ceph_mds_root_rfiles" > "$DATA_DIR/prom-mds-rfiles.json" 2>/dev/null || true
    
    local fs_list
    fs_list=$(rook_exec ceph fs ls --format json 2>/dev/null || echo '[]')
    write_json_file "$DATA_DIR/cephfs-list.json" "$fs_list"

    local fs_names
    fs_names=$(echo "$fs_list" | jq -r '.[].name')

    if [[ -z "$fs_names" ]]; then
        append_line "No CephFS filesystems detected"
        return
    fi

    for fs in $fs_names; do
        subsection "Filesystem: $fs"
        
        # Try to get metrics from Prometheus first
        local total_bytes total_files
        total_bytes=$(jq -r --arg fs "$fs" '.data.result[]? | select(.metric.fs_id == $fs or .metric.ceph_daemon | contains($fs)) | .value[1]' \
            "$DATA_DIR/prom-mds-rbytes.json" 2>/dev/null | head -1 || echo "0")
        total_files=$(jq -r --arg fs "$fs" '.data.result[]? | select(.metric.fs_id == $fs or .metric.ceph_daemon | contains($fs)) | .value[1]' \
            "$DATA_DIR/prom-mds-rfiles.json" 2>/dev/null | head -1 || echo "0")
        
        if [[ "$total_bytes" != "0" ]] && [[ -n "$total_bytes" ]]; then
            append_line "Total managed data: $(human_bytes "$total_bytes")"
            append_line "Total files: $total_files"
        fi
        
        rook_exec ceph fs status "$fs" | tee -a "$REPORT_FILE" || true

        local groups_json
        groups_json=$(rook_exec ceph fs subvolumegroup ls "$fs" --format json 2>/dev/null || echo '[]')
        local groups
        groups=$(echo "$groups_json" | jq -r '.[].name')

        for group in $groups; do
            local subvols_json
            subvols_json=$(rook_exec ceph fs subvolume ls "$fs" "$group" --format json 2>/dev/null || echo '[]')
            local count
            count=$(echo "$subvols_json" | jq 'length')
            printf "    Group %s: %s subvolumes\n" "$group" "$count" | tee -a "$REPORT_FILE"
            echo "$subvols_json" | jq -c --arg fs "$fs" --arg group "$group" '.[] | {fs:$fs,group:$group,name:.name,bytes_quota:(.bytes_quota // 0)}' >> "$cephfs_ndjson"
        done
    done
}

collect_rgw_data() {
    section "RGW Buckets"
    
    # Build enhanced OBC bucket mapping - DUAL sources
    log_line "Building OBC-to-bucket mapping (spec + secrets)"
    
    # Source 1: Direct from OBC spec.bucketName  
    jq -r '.items[]? | .spec.bucketName // empty' "$DATA_DIR/obc.json" 2>/dev/null \
        | sed '/^$/d' > "$DATA_DIR/obc-buckets-spec.txt"
    
    # Source 2: Extract from Secrets (for manually created or orphaned buckets)
    oc get secrets --all-namespaces \
        -l bucket-provisioner=openshift-storage.ceph.rook.io \
        -o json 2>/dev/null | \
        jq -r '.items[]? | .data.BUCKET_NAME // empty | @base64d' 2>/dev/null \
        | sed '/^$/d' > "$DATA_DIR/obc-buckets-secrets.txt" || touch "$DATA_DIR/obc-buckets-secrets.txt"
    
    # Combine and deduplicate
    cat "$DATA_DIR/obc-buckets-spec.txt" "$DATA_DIR/obc-buckets-secrets.txt" 2>/dev/null \
        | sort -u > "$obc_bucket_map"
    
    local bucket_list
    bucket_list=$(rook_exec radosgw-admin bucket list --format json 2>/dev/null || echo '[]')
    write_json_file "$DATA_DIR/rgw-buckets-list.json" "$bucket_list"

    local buckets
    buckets=$(echo "$bucket_list" | jq -r '.[]')

    if [[ -z "$buckets" ]]; then
        append_line "No RGW buckets detected (RGW may be disabled)"
        return
    fi

    local total=0 orphan=0 loki_count=0

    for bucket in $buckets; do
        local stats_json size_kb size_bytes objects owner has_obc has_obc_spec has_obc_secret loki_flag logging_flag
        stats_json=$(rook_exec radosgw-admin bucket stats --bucket="$bucket" --format json 2>/dev/null || echo '{}')
        size_kb=$(echo "$stats_json" | jq '.usage["rgw.main"].size_kb_actual // 0')
        size_bytes=$((size_kb * 1024))
        objects=$(echo "$stats_json" | jq '.usage["rgw.main"].num_objects // 0')
        owner=$(echo "$stats_json" | jq -r '.owner // "unknown"')

        # Check both OBC sources
        has_obc_spec=false
        has_obc_secret=false
        has_obc=false
        
        if [[ -s "$DATA_DIR/obc-buckets-spec.txt" ]] && grep -Fxq "$bucket" "$DATA_DIR/obc-buckets-spec.txt"; then
            has_obc_spec=true
            has_obc=true
        fi
        if [[ -s "$DATA_DIR/obc-buckets-secrets.txt" ]] && grep -Fxq "$bucket" "$DATA_DIR/obc-buckets-secrets.txt"; then
            has_obc_secret=true
            has_obc=true
        fi

        # Check for Loki/logging patterns
        loki_flag=false
        logging_flag=false
        if [[ "$bucket" =~ (loki|logging) ]]; then
            loki_flag=true
            ((loki_count++))
        fi
        if [[ "$bucket" =~ (log|logs) ]]; then
            logging_flag=true
        fi

        jq -n \
            --arg bucket "$bucket" \
            --arg owner "$owner" \
            --argjson size_bytes "$size_bytes" \
            --argjson objects "$objects" \
            --argjson has_obc "$has_obc" \
            --argjson has_obc_spec "$has_obc_spec" \
            --argjson has_obc_secret "$has_obc_secret" \
            --argjson loki "$loki_flag" \
            --argjson logging "$logging_flag" \
            '{name:$bucket,owner:$owner,size_bytes:$size_bytes,objects:$objects,has_obc:$has_obc,has_obc_spec:$has_obc_spec,has_obc_secret:$has_obc_secret,tags:{loki:$loki,logging:$logging}}' >> "$rgw_ndjson"

        printf "  %-50s %12s | objects: %-10s | OBC: %s" "$bucket" "$(human_bytes "$size_bytes")" "$objects" "$has_obc"
        if [[ "$loki_flag" == true ]]; then
            printf " | LOKI"
        fi
        printf "\n" | tee -a "$REPORT_FILE"

        ((total++))
        if [[ "$has_obc" == false ]]; then
            ((orphan++))
        fi
    done

    append_line "Total buckets: $total"
    append_line "Buckets without OBC: $orphan"
    if [[ $loki_count -gt 0 ]]; then
        append_line "Loki/logging buckets: $loki_count (may be operational)"
    fi
}

write_json_artifacts() {
    write_json_array "$rbd_ndjson" "$DATA_DIR/rbd-images.json"
    write_json_array "$rgw_ndjson" "$DATA_DIR/rgw-buckets.json"
    write_json_array "$cephfs_ndjson" "$DATA_DIR/cephfs-subvols.json"
}

build_orphans_json() {
    local orphan_json="$DATA_DIR/orphans.json"
    
    # Use Prometheus pool data if available, fallback to ceph-df-detail
    local pools_source="$DATA_DIR/prom-pool-bytes-used.json"
    if [[ ! -f "$pools_source" ]]; then
        pools_source="$POOL_STATS_JSON"
    fi

    jq -n \
        --argfile rbd "$DATA_DIR/rbd-images.json" \
        --argfile rgw "$DATA_DIR/rgw-buckets.json" \
        '{
            rbd: {
                no_pv: [ $rbd[] | select(.has_pv == false) ],
                no_watchers: [ $rbd[] | select(.watcher_count == 0) ],
                test_pattern: [ $rbd[] | select(.test_pattern == true) ],
                high_confidence_orphans: [ $rbd[] | select(
                    .has_pv == false and 
                    .watcher_count == 0 and 
                    .size_bytes > 0
                ) ]
            },
            rgw: {
                no_obc: [ $rgw[] | select(.has_obc == false) ],
                no_obc_no_secret: [ $rgw[] | select(
                    .has_obc_spec == false and 
                    .has_obc_secret == false
                ) ],
                loki_related: [ $rgw[] | select(.tags.loki == true) ],
                empty_no_obc: [ $rgw[] | select(
                    .has_obc == false and 
                    .size_bytes == 0
                ) ],
                high_confidence_orphans: [ $rgw[] | select(
                    .has_obc == false and 
                    .tags.loki == false and 
                    .tags.logging == false
                ) ]
            },
            summary: {
                rbd_orphan_count: [ $rbd[] | select(.has_pv == false) ] | length,
                rbd_high_confidence: [ $rbd[] | select(.has_pv == false and .watcher_count == 0) ] | length,
                rgw_orphan_count: [ $rgw[] | select(.has_obc == false) ] | length,
                rgw_high_confidence: [ $rgw[] | select(
                    .has_obc == false and 
                    .tags.loki == false
                ) ] | length
            }
        }' > "$orphan_json"
    
    log_line "Orphan detection complete - see $orphan_json"
}

build_report_json() {
    # Read Prometheus metrics if available
    local total_raw_bytes total_used_bytes total_stored_bytes efficiency_ratio
    if [[ -f "$DATA_DIR/prom-cluster-total.json" ]]; then
        total_raw_bytes=$(extract_metric_value "$(cat "$DATA_DIR/prom-cluster-total.json")" || echo "0")
        total_used_bytes=$(extract_metric_value "$(cat "$DATA_DIR/prom-cluster-used.json")" || echo "0")
        total_stored_bytes=$(extract_metric_value "$(cat "$DATA_DIR/prom-cluster-stored.json")" || echo "0")
        
        if [[ "$total_used_bytes" != "0" ]] && [[ -n "$total_used_bytes" ]]; then
            efficiency_ratio=$(echo "scale=3; $total_stored_bytes / $total_used_bytes" | bc -l)
        else
            efficiency_ratio="0"
        fi
    else
        # Fallback to ceph-df.json
        total_raw_bytes=$(jq -r '.stats.total_bytes // 0' "$DATA_DIR/ceph-df.json")
        total_used_bytes=$(jq -r '.stats.total_used_bytes // 0' "$DATA_DIR/ceph-df.json")
        total_stored_bytes="0"
        efficiency_ratio="0"
    fi

    jq -n \
        --arg generated "$(timestamp)" \
        --arg output "$AUDIT_DIR" \
        --arg prom_endpoint "${PROMETHEUS_ENDPOINT:-none}" \
        --arg total_raw "$total_raw_bytes" \
        --arg total_used "$total_used_bytes" \
        --arg total_stored "$total_stored_bytes" \
        --arg efficiency "$efficiency_ratio" \
        --argfile cluster "$DATA_DIR/ceph-status.json" \
        --argfile rbd "$DATA_DIR/rbd-images.json" \
        --argfile rgw "$DATA_DIR/rgw-buckets.json" \
        --argfile cephfs "$DATA_DIR/cephfs-subvols.json" \
        --argfile pv "$DATA_DIR/pv.json" \
        --argfile pvc "$DATA_DIR/pvc.json" \
        --argfile obc "$DATA_DIR/obc.json" \
        --argfile orphans "$DATA_DIR/orphans.json" \
        '{
            generated_at: $generated,
            output_dir: $output,
            data_sources: {
                prometheus: $prom_endpoint,
                kubernetes: "oc CLI authenticated",
                ceph_direct: "rook-ceph-tools pod (supplemental)"
            },
            cluster: {
                health: ($cluster.health.status // "unknown"),
                osds: ($cluster.osdmap.num_osds // 0),
                mons: ($cluster.monmap.mons | length // 0),
                capacity: {
                    raw_total_bytes: ($total_raw | tonumber),
                    raw_used_bytes: ($total_used | tonumber),
                    stored_bytes: ($total_stored | tonumber),
                    efficiency_ratio: ($efficiency | tonumber)
                }
            },
            stats: {
                rbd_images: ($rbd | length),
                rbd_orphans: ($orphans.rbd.no_pv | length),
                rbd_high_confidence_orphans: ($orphans.rbd.high_confidence_orphans | length),
                rgw_buckets: ($rgw | length),
                rgw_orphans: ($orphans.rgw.no_obc | length),
                rgw_high_confidence_orphans: ($orphans.rgw.high_confidence_orphans | length),
                loki_buckets: ($orphans.rgw.loki_related | length),
                pv_total: ($pv.items | length),
                pvc_total: ($pvc.items | length),
                obc_total: ($obc.items | length)
            },
            datasets: {
                rbd: $rbd,
                rgw: $rgw,
                cephfs: $cephfs
            },
            orphans: $orphans
        }' > "$JSON_FILE"
}

write_summary() {
    jq -r '
        "=== ODF STORAGE AUDIT SUMMARY ===",
        "Generated: \(.generated_at)",
        "",
        "Cluster health: \(.cluster.health)",
        "OSDs: \(.cluster.osds) | MONs: \(.cluster.mons)",
        "",
        "Pools tracked: \(.stats.pools)",
        "RBD images: \(.stats.rbd_images) | Orphans: \(.stats.rbd_orphans)",
        "RGW buckets: \(.stats.rgw_buckets) | Orphans: \(.stats.rgw_orphans)",
        "Loki buckets: \(.stats.loki_buckets)",
        "Suspected test/benchmark pools: \(.stats.suspected_pools)",
        "",
        "K8s PVs: \(.stats.pv_total) | PVCs: \(.stats.pvc_total) | OBCs: \(.stats.obc_total)",
        "",
        "Full report: \(.output_dir)/report.txt",
        "JSON data: \(.output_dir)/report.json"
    ' "$JSON_FILE" > "$SUMMARY_FILE"

    cat "$SUMMARY_FILE"
}

print_cleanup_hints() {
    section "Orphan Detection Summary"
    
    # HIGH CONFIDENCE orphans
    subsection "High Confidence Orphans (safe to review for cleanup)"
    jq -r '.orphans.rbd.high_confidence_orphans[]? | "RBD: \(.pool)/\(.image) - \(.size_human) - no PV, no watchers"' "$JSON_FILE" \
        | sed 's/^/  ✗ /' | tee -a "$REPORT_FILE" || append_line "  (none)"
    
    jq -r '.orphans.rgw.high_confidence_orphans[]? | "RGW: \(.name) - \(.size_bytes) bytes - no OBC, not loki/logging"' "$JSON_FILE" \
        | sed 's/^/  ✗ /' | tee -a "$REPORT_FILE" || true
    
    # MEDIUM CONFIDENCE - needs manual review
    subsection "Requires Manual Review"
    jq -r '.orphans.rbd.no_pv[]? | select(.watcher_count > 0) | "RBD: \(.pool)/\(.image) - \(.size_human) - no PV but has \(.watcher_count) watcher(s)"' "$JSON_FILE" \
        | sed 's/^/  ⚠ /' | tee -a "$REPORT_FILE" || true
    
    jq -r '.orphans.rgw.loki_related[]? | "RGW: \(.name) - \(.size_bytes) bytes - Loki/logging bucket (may be operational)"' "$JSON_FILE" \
        | sed 's/^/  ⚠ /' | tee -a "$REPORT_FILE" || true
    
    jq -r '.orphans.rbd.test_pattern[]? | "RBD: \(.pool)/\(.image) - test/benchmark pattern name"' "$JSON_FILE" \
        | sed 's/^/  ⚠ /' | tee -a "$REPORT_FILE" || true
    
    # Summary counts
    subsection "Summary"
    jq -r '"RBD orphans (no PV): \(.orphans.summary.rbd_orphan_count // 0)"' "$JSON_FILE" | tee -a "$REPORT_FILE"
    jq -r '"RBD high confidence: \(.orphans.summary.rbd_high_confidence // 0)"' "$JSON_FILE" | tee -a "$REPORT_FILE"
    jq -r '"RGW orphans (no OBC): \(.orphans.summary.rgw_orphan_count // 0)"' "$JSON_FILE" | tee -a "$REPORT_FILE"
    jq -r '"RGW high confidence: \(.orphans.summary.rgw_high_confidence // 0)"' "$JSON_FILE" | tee -a "$REPORT_FILE"
}

generate_orphan_candidates_report() {
    local report="$AUDIT_DIR/potential-orphans.txt"
    
    log_line "Generating orphan candidates report for manual review"
    
    cat > "$report" <<'HEADER'
=================================================================================
POTENTIAL ORPHANED RESOURCES - MANUAL REVIEW REQUIRED
=================================================================================

This report categorizes potential orphaned resources by confidence level.
Review each section carefully before taking any cleanup actions.

CONFIDENCE LEVELS:
  HIGH       - Strong evidence of orphan status, low risk of disruption
  MEDIUM     - Some evidence, requires verification before cleanup
  LOW        - May be in use, proceed with extreme caution

HEADER

    # HIGH CONFIDENCE RBD IMAGES
    echo "" >> "$report"
    echo "=== HIGH CONFIDENCE: ORPHANED RBD IMAGES ===" >> "$report"
    echo "" >> "$report"
    echo "These RBD images have NO PersistentVolume, NO watchers, and contain data." >> "$report"
    echo "They are safe candidates for cleanup after verification." >> "$report"
    echo "" >> "$report"
    
    jq -r '.orphans.rbd.high_confidence_orphans[]? | 
        "  \(.pool)/\(.image)\n    Size: \(.size_human)\n    Watchers: \(.watcher_count)\n    Has PV: \(.has_pv)\n"' \
        "$JSON_FILE" >> "$report" 2>/dev/null || echo "  (none detected)" >> "$report"
    
    # HIGH CONFIDENCE RGW BUCKETS
    echo "" >> "$report"
    echo "=== HIGH CONFIDENCE: ORPHANED RGW BUCKETS ===" >> "$report"
    echo "" >> "$report"
    echo "These buckets have NO ObjectBucketClaim and are NOT Loki/logging related." >> "$report"
    echo "" >> "$report"
    
    jq -r '.orphans.rgw.high_confidence_orphans[]? | 
        "  \(.name)\n    Size: \(.size_bytes) bytes\n    Objects: \(.objects)\n    Owner: \(.owner)\n"' \
        "$JSON_FILE" >> "$report" 2>/dev/null || echo "  (none detected)" >> "$report"
    
    # MEDIUM CONFIDENCE RBD
    echo "" >> "$report"
    echo "=== MEDIUM CONFIDENCE: RBD IMAGES (verify before cleanup) ===" >> "$report"
    echo "" >> "$report"
    echo "These images have NO PersistentVolume but HAVE active watchers." >> "$report"
    echo "Verify watchers are not stale before considering cleanup." >> "$report"
    echo "" >> "$report"
    
    jq -r '.orphans.rbd.no_pv[]? | select(.watcher_count > 0) | 
        "  \(.pool)/\(.image)\n    Size: \(.size_human)\n    Watchers: \(.watcher_count)\n    Check: rbd status \(.pool)/\(.image)\n"' \
        "$JSON_FILE" >> "$report" 2>/dev/null || echo "  (none detected)" >> "$report"
    
    # MEDIUM CONFIDENCE RGW
    echo "" >> "$report"
    echo "=== MEDIUM CONFIDENCE: RGW BUCKETS (Loki/Logging related) ===" >> "$report"
    echo "" >> "$report"
    echo "These buckets have NO OBC but appear to be Loki/logging buckets." >> "$report"
    echo "They may be operational. Verify with logging team before cleanup." >> "$report"
    echo "" >> "$report"
    
    jq -r '.orphans.rgw.loki_related[]? | 
        "  \(.name)\n    Size: \(.size_bytes) bytes\n    Objects: \(.objects)\n    Has OBC: \(.has_obc)\n"' \
        "$JSON_FILE" >> "$report" 2>/dev/null || echo "  (none detected)" >> "$report"
    
    # LOW CONFIDENCE - Test patterns
    echo "" >> "$report"
    echo "=== LOW CONFIDENCE: TEST/BENCHMARK PATTERN IMAGES ===" >> "$report"
    echo "" >> "$report"
    echo "These images have test/benchmark/fio patterns in their names." >> "$report"
    echo "They may still be in active use. Verify with dev/ops teams." >> "$report"
    echo "" >> "$report"
    
    jq -r '.orphans.rbd.test_pattern[]? | 
        "  \(.pool)/\(.image)\n    Size: \(.size_human)\n    Watchers: \(.watcher_count)\n    Has PV: \(.has_pv)\n"' \
        "$JSON_FILE" >> "$report" 2>/dev/null || echo "  (none detected)" >> "$report"
    
    # VERIFICATION STEPS
    cat >> "$report" <<'FOOTER'

=================================================================================
VERIFICATION STEPS BEFORE CLEANUP
=================================================================================

Before deleting ANY resource, complete these verification steps:

1. Check cluster events for the resource:
   oc get events -A | grep <resource-name>

2. Verify no active pods are using the resource:
   oc get pods -A -o wide | grep <resource-name>

3. For RBD images, check watchers:
   oc rsh -n openshift-storage <rook-ceph-tools-pod> rbd status <pool>/<image>

4. For buckets, search application configs:
   oc get cm,secrets -A -o yaml | grep <bucket-name>

5. Wait 7 days and re-run audit to confirm resource is still orphaned.

6. Use odf-cleanup-generator.sh to generate safe cleanup scripts.

=================================================================================
IMPORTANT NOTES
=================================================================================

- This report is for MANUAL REVIEW only
- Do NOT automate cleanup based on this report alone
- Always verify in a staging environment first if possible
- Keep backups of any data before deletion
- Coordinate with application teams before cleanup

Generated: $(timestamp)
Report path: $report

=================================================================================
FOOTER

    log_line "Orphan candidates report written to: $report"
    section "Orphan Candidates Report"
    append_line "Detailed review file: $report"
    append_line "Review this file manually before executing any cleanup."
}

main() {
    log_line "Starting audit in $AUDIT_DIR"
    init_header
    collect_k8s_data
    collect_ceph_cluster
    collect_rbd_data
    collect_cephfs_data
    collect_rgw_data
    write_json_artifacts
    build_orphans_json
    build_report_json
    generate_orphan_candidates_report
    write_summary
    print_cleanup_hints
    append_line "\nAudit complete. Artifacts in: $AUDIT_DIR"
}

main "$@"