#!/bin/bash
#
# Shared helper functions for ODF helper scripts.
#

if [[ -n "${ODF_COMMON_SH:-}" ]]; then
    return 0
fi

ODF_COMMON_SH=1

require_commands() {
    local missing=()
    for cmd in "$@"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        printf 'ERROR: Missing required commands: %s\n' "${missing[*]}" >&2
        exit 1
    fi
}

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

human_bytes() {
    local bytes=$1
    local scale=2
    if [[ -z "$bytes" ]] || [[ "$bytes" -eq 0 ]]; then
        echo "0 B"
        return
    fi

    local units=("B" "KiB" "MiB" "GiB" "TiB" "PiB")
    local idx=0
    local value="$bytes"

    while [[ $(echo "$value >= 1024" | bc) -eq 1 && $idx -lt ${#units[@]}-1 ]]; do
        value=$(echo "scale=${scale}; $value / 1024" | bc -l)
        ((idx++))
    done

    printf "%.*f %s" "$scale" "$value" "${units[$idx]}"
}

ensure_rook_tools_pod() {
    if [[ -n "${ROOK_TOOLS_POD:-}" ]]; then
        return
    fi

    ROOK_TOOLS_POD=$(oc get pod -n openshift-storage -l app=rook-ceph-tools \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

    if [[ -z "$ROOK_TOOLS_POD" ]]; then
        echo "ERROR: rook-ceph-tools pod not found in openshift-storage namespace" >&2
        exit 1
    fi
}

rook_exec() {
    ensure_rook_tools_pod
    oc rsh -n openshift-storage "$ROOK_TOOLS_POD" "$@" 2>&1
}

write_json_array() {
    local ndjson_file=$1
    local output_file=$2

    if [[ -s "$ndjson_file" ]]; then
        jq -s '.' "$ndjson_file" > "$output_file"
    else
        echo "[]" > "$output_file"
    fi
}

log_info() {
    printf "[%s] %s\n" "$(timestamp)" "$*"
}

