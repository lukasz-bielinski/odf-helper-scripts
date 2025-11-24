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
    log_line "Collecting Ceph cluster metadata"
    rook_exec ceph status --format json > "$DATA_DIR/ceph-status.json"
    rook_exec ceph df --format json > "$DATA_DIR/ceph-df.json"
    rook_exec ceph df detail --format json > "$POOL_STATS_JSON"
    rook_exec rados df --format json > "$DATA_DIR/rados-df.json"

    section "Ceph Cluster Status"
    local health osds mons total_bytes used_bytes
    health=$(jq -r '.health.status // "unknown"' "$DATA_DIR/ceph-status.json")
    osds=$(jq '.osdmap.num_osds // 0' "$DATA_DIR/ceph-status.json")
    mons=$(jq '.monmap.mons | length // 0' "$DATA_DIR/ceph-status.json")
    total_bytes=$(jq '.stats.total_bytes // 0' "$DATA_DIR/ceph-df.json")
    used_bytes=$(jq '.stats.total_used_bytes // 0' "$DATA_DIR/ceph-df.json")

    append_line "Health: $health"
    append_line "MONs: $mons | OSDs: $osds"
    append_line "Usage: $(human_bytes "$used_bytes") / $(human_bytes "$total_bytes")"

    subsection "Top pools by usage"
    local total_cluster_bytes
    total_cluster_bytes=$(jq '.stats.total_bytes // 0' "$DATA_DIR/ceph-df.json")
    jq -r --argjson total_bytes "$total_cluster_bytes" '
        def normalize_bytes(val, field_name):
            val as $v
            | if ($v == 0 or $v == null) then 0
              elif ($total_bytes > 0 and $v > $total_bytes) then
                  # Value exceeds cluster total - likely in KB, convert to bytes
                  ($v * 1024)
              else
                  # Assume bytes
                  $v
              end;
        def pool_entries:
            (.pools // []) | map({
                pool_name:(.name // "unknown"),
                bytes_used:(
                    if (.stats.bytes_used? != null) then normalize_bytes(.stats.bytes_used | tonumber, "bytes_used")
                    elif (.stats.kb_used? != null) then (.stats.kb_used | tonumber) * 1024
                    elif (.stats.stored? != null) then normalize_bytes(.stats.stored | tonumber, "stored")
                    else 0 end)
            });
        pool_entries[]
        | [ .pool_name, (.bytes_used/1024/1024) ]
        | @tsv
    ' "$POOL_STATS_JSON" \
        | sort -k2 -n -r | head -10 \
        | awk '{printf "  %-35s %12.2f MiB\n", $1, $2}' \
        | tee -a "$REPORT_FILE"
}

collect_rbd_data() {
    section "RBD Pools & Images"
    local pools
    pools=$(jq -r '(.pools // [])[]? | .name | select(test("(rbd|block)", "i"))' "$POOL_STATS_JSON" | sort -u)
    if [[ -z "$pools" ]]; then
        pools=$(rook_exec ceph osd pool ls --format json 2>/dev/null | jq -r '.[]' | grep -E 'rbd|block' || true)
    fi
    if [[ -z "$pools" ]]; then
        pools=$(rook_exec ceph osd pool ls 2>/dev/null | tr -d '[]",' | tr ' ' '\n' | grep -E 'rbd|block' || true)
    fi

    if [[ -z "$pools" ]]; then
        append_line "No RBD pools detected"
        return
    fi

    local total_images=0
    local orphan_images=0

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
            local info_json status_json size_bytes size_human watcher_count watchers_json has_pv manual_flag
            info_json=$(rook_exec rbd info "$pool/$image" --format json 2>/dev/null || echo '{}')
            size_bytes=$(echo "$info_json" | jq '.size // 0')
            size_human=$(human_bytes "$size_bytes")
            status_json=$(rook_exec rbd status "$pool/$image" --format json 2>/dev/null || echo '{}')
            watcher_count=$(echo "$status_json" | jq '.watchers | length // 0')
            watchers_json=$(echo "$status_json" | jq -c '.watchers // []')

            has_pv=false
            if [[ -s "$pv_volume_map" ]] && grep -Fq "$image" "$pv_volume_map"; then
                has_pv=true
            fi

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
                '{pool:$pool,image:$image,size_bytes:$size_bytes,size_human:$size_human,watchers:$watchers,watcher_count:$watcher_count,has_pv:$has_pv,manual_flag:$manual}' >> "$rbd_ndjson"

            printf "  %-60s %12s | watchers: %s | bound: %s\n" "$pool/$image" "$size_human" "$watcher_count" "$has_pv" | tee -a "$REPORT_FILE"

            ((total_images++))
            if [[ "$has_pv" == false ]]; then
                ((orphan_images++))
            fi
        done
    done

    append_line "Total RBD images: $total_images"
    append_line "Potential orphaned images: $orphan_images"
}

collect_cephfs_data() {
    section "CephFS"
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
    local bucket_list
    bucket_list=$(rook_exec radosgw-admin bucket list --format json 2>/dev/null || echo '[]')
    write_json_file "$DATA_DIR/rgw-buckets-list.json" "$bucket_list"

    local buckets
    buckets=$(echo "$bucket_list" | jq -r '.[]')

    if [[ -z "$buckets" ]]; then
        append_line "No RGW buckets detected (RGW may be disabled)"
        return
    fi

    local total=0 orphan=0

    for bucket in $buckets; do
        local stats_json size_kb size_bytes objects owner has_obc loki_flag
        stats_json=$(rook_exec radosgw-admin bucket stats --bucket="$bucket" --format json 2>/dev/null || echo '{}')
        size_kb=$(echo "$stats_json" | jq '.usage["rgw.main"].size_kb_actual // 0')
        size_bytes=$((size_kb * 1024))
        objects=$(echo "$stats_json" | jq '.usage["rgw.main"].num_objects // 0')
        owner=$(echo "$stats_json" | jq -r '.owner // "unknown"')

        has_obc=false
        if [[ -s "$obc_bucket_map" ]] && grep -Fxq "$bucket" "$obc_bucket_map"; then
            has_obc=true
        fi

        loki_flag=false
        if [[ "$bucket" =~ [Ll]oki ]]; then
            loki_flag=true
        fi

        jq -n \
            --arg bucket "$bucket" \
            --arg owner "$owner" \
            --argjson size_bytes "$size_bytes" \
            --argjson objects "$objects" \
            --argjson has_obc "$has_obc" \
            --argjson loki "$loki_flag" \
            '{name:$bucket,owner:$owner,size_bytes:$size_bytes,objects:$objects,has_obc:$has_obc,tags:{loki:$loki}}' >> "$rgw_ndjson"

        printf "  %-50s %12s | objects: %-10s | OBC: %s\n" "$bucket" "$(human_bytes "$size_bytes")" "$objects" "$has_obc" | tee -a "$REPORT_FILE"

        ((total++))
        if [[ "$has_obc" == false ]]; then
            ((orphan++))
        fi
    done

    append_line "Total buckets: $total"
    append_line "Buckets without OBC: $orphan"
}

write_json_artifacts() {
    write_json_array "$rbd_ndjson" "$DATA_DIR/rbd-images.json"
    write_json_array "$rgw_ndjson" "$DATA_DIR/rgw-buckets.json"
    write_json_array "$cephfs_ndjson" "$DATA_DIR/cephfs-subvols.json"
}

build_orphans_json() {
    local pools_json orphan_json
    pools_json="$POOL_STATS_JSON"
    orphan_json="$DATA_DIR/orphans.json"

    jq -n \
        --argfile rbd "$DATA_DIR/rbd-images.json" \
        --argfile rgw "$DATA_DIR/rgw-buckets.json" \
        --argfile pools "$pools_json" \
        'def normalize_bytes($val, $total_bytes):
            if ($val == 0 or $val == null) then 0
            elif ($total_bytes > 0 and $val > $total_bytes) then ($val * 1024)
            else $val end;
        def pool_entries($p):
            ($p.pools // []) | map({
                pool_name:(.name // "unknown"),
                bytes_used:(
                    if (.stats.bytes_used? != null) then normalize_bytes(.stats.bytes_used | tonumber, $p.stats.total_bytes // 0)
                    elif (.stats.kb_used? != null) then (.stats.kb_used | tonumber) * 1024
                    elif (.stats.stored? != null) then normalize_bytes(.stats.stored | tonumber, $p.stats.total_bytes // 0)
                    else 0 end)
            });
        {
            rbd: [ $rbd[] | select(.has_pv | not) ],
            rgw: [ $rgw[] | select(.has_obc | not) ],
            loki_buckets: [ $rgw[] | select(.tags.loki == true) ],
            suspected_pools: [ pool_entries($pools)[] | select((.pool_name // "") | test("(test|bench|perf|fio|tmp|temp)", "i")) | {name:(.pool_name // "unknown"),size_mib:(.bytes_used/1024/1024)} ]
        }' > "$orphan_json"
}

build_report_json() {
    jq -n \
        --arg generated "$(timestamp)" \
        --arg output "$AUDIT_DIR" \
        --argfile cluster "$DATA_DIR/ceph-status.json" \
        --argfile cluster_df "$DATA_DIR/ceph-df.json" \
        --argfile rados "$POOL_STATS_JSON" \
        --argfile rbd "$DATA_DIR/rbd-images.json" \
        --argfile rgw "$DATA_DIR/rgw-buckets.json" \
        --argfile cephfs "$DATA_DIR/cephfs-subvols.json" \
        --argfile pv "$DATA_DIR/pv.json" \
        --argfile pvc "$DATA_DIR/pvc.json" \
        --argfile obc "$DATA_DIR/obc.json" \
        --argfile orphans "$DATA_DIR/orphans.json" \
        'def normalize_bytes($val, $total_bytes):
            if ($val == 0 or $val == null) then 0
            elif ($total_bytes > 0 and $val > $total_bytes) then ($val * 1024)
            else $val end;
        def pool_entries($p):
            ($p.pools // []) | map({
                pool_name:(.name // "unknown"),
                kb_used:(
                    if (.stats.kb_used? != null) then (.stats.kb_used | tonumber)
                    elif (.stats.bytes_used? != null) then (normalize_bytes(.stats.bytes_used | tonumber, $p.stats.total_bytes // 0) / 1024)
                    elif (.stats.stored? != null) then (normalize_bytes(.stats.stored | tonumber, $p.stats.total_bytes // 0) / 1024)
                    else 0 end)
            });
        {
            generated_at:$generated,
            output_dir:$output,
            cluster:{
                health:($cluster.health.status // "unknown"),
                osds:($cluster.osdmap.num_osds // 0),
                mons:($cluster.monmap.mons | length // 0)
            },
            stats:{
                pools:(pool_entries($rados) | length),
                rbd_images:($rbd | length),
                rbd_orphans:($orphans.rbd | length),
                rgw_buckets:($rgw | length),
                rgw_orphans:($orphans.rgw | length),
                loki_buckets:($orphans.loki_buckets | length),
                suspected_pools:($orphans.suspected_pools | length),
                pv_total:($pv.items | length),
                pvc_total:($pvc.items | length),
                obc_total:($obc.items | length)
            },
            datasets:{
                pools:pool_entries($rados),
                rbd:$rbd,
                rgw:$rgw,
                cephfs:$cephfs
            },
            orphans:$orphans
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
    section "Potential cleanup candidates"
    jq -r '.orphans.rbd[]? | "RBD orphan: \(.pool)/\(.image) size \(.size_human)"' "$JSON_FILE" | sed 's/^/  - /' | tee -a "$REPORT_FILE"
    jq -r '.orphans.rgw[]? | "RGW bucket without OBC: \(.name) size " + (.size_bytes | tostring)' "$JSON_FILE" | sed 's/^/  - /' | tee -a "$REPORT_FILE"
    jq -r '.orphans.suspected_pools[]? | "Suspect pool: \(.name) (size=\(.size_mib) MiB)"' "$JSON_FILE" | sed 's/^/  - /' | tee -a "$REPORT_FILE"
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
    write_summary
    print_cleanup_hints
    append_line "\nAudit complete. Artifacts in: $AUDIT_DIR"
}

main "$@"