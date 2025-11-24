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

# =============================================================================
# Prometheus Access Layer
# =============================================================================

# Global variables for Prometheus endpoint (cached after discovery)
PROMETHEUS_ENDPOINT=""
PROMETHEUS_TOKEN=""

discover_prometheus_endpoint() {
    if [[ -n "$PROMETHEUS_ENDPOINT" ]]; then
        return 0
    fi

    log_info "Discovering Prometheus endpoint..."

    # Method 1: Try Thanos Querier route (standard OpenShift monitoring)
    local thanos_host
    thanos_host=$(oc get route thanos-querier -n openshift-monitoring \
        -o jsonpath='{.spec.host}' 2>/dev/null || echo "")
    
    if [[ -n "$thanos_host" ]]; then
        PROMETHEUS_ENDPOINT="https://$thanos_host"
        PROMETHEUS_TOKEN=$(oc whoami -t 2>/dev/null || echo "")
        if [[ -z "$PROMETHEUS_TOKEN" ]]; then
            echo "" >&2
            echo "========================================================================" >&2
            echo "FATAL ERROR: Prometheus endpoint found but no authentication token!" >&2
            echo "========================================================================" >&2
            echo "" >&2
            echo "Thanos Querier route found: $PROMETHEUS_ENDPOINT" >&2
            echo "But 'oc whoami -t' returned empty token!" >&2
            echo "" >&2
            echo "Verify manually:" >&2
            echo "  oc whoami -t" >&2
            echo "  oc auth can-i get routes -n openshift-monitoring" >&2
            echo "" >&2
            exit 1
        fi
        log_info "✓ Using Thanos Querier: $PROMETHEUS_ENDPOINT"
        return 0
    fi

    # Method 2: Try Prometheus service in openshift-storage
    local prom_svc
    prom_svc=$(oc get service prometheus-operated -n openshift-storage \
        -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "")
    
    if [[ -n "$prom_svc" ]]; then
        local prom_port
        prom_port=$(oc get service prometheus-operated -n openshift-storage \
            -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || echo "9090")
        PROMETHEUS_ENDPOINT="http://$prom_svc:$prom_port"
        PROMETHEUS_TOKEN=$(oc whoami -t 2>/dev/null || echo "")
        log_info "✓ Using Prometheus service: $PROMETHEUS_ENDPOINT"
        return 0
    fi

    # No endpoint found - PANIC
    echo "" >&2
    echo "========================================================================" >&2
    echo "FATAL ERROR: Cannot discover Prometheus endpoint!" >&2
    echo "========================================================================" >&2
    echo "" >&2
    echo "Tried methods:" >&2
    echo "  1. Thanos Querier route in openshift-monitoring → NOT FOUND" >&2
    echo "  2. Prometheus service in openshift-storage → NOT FOUND" >&2
    echo "" >&2
    echo "Verify manually:" >&2
    echo "  # Check Thanos Querier:" >&2
    echo "  oc get route thanos-querier -n openshift-monitoring" >&2
    echo "" >&2
    echo "  # Check Prometheus service:" >&2
    echo "  oc get service prometheus-operated -n openshift-storage" >&2
    echo "" >&2
    echo "  # Check your permissions:" >&2
    echo "  oc auth can-i get routes -n openshift-monitoring" >&2
    echo "  oc auth can-i get services -n openshift-storage" >&2
    echo "" >&2
    exit 1
}

query_prometheus() {
    local query=$1
    local time_range=${2:-}
    
    discover_prometheus_endpoint

    local api_path="/api/v1/query"
    local url="${PROMETHEUS_ENDPOINT}${api_path}"
    
    # URL-encode the query
    local encoded_query
    encoded_query=$(printf '%s' "$query" | jq -sRr @uri)
    
    local full_url="${url}?query=${encoded_query}"
    if [[ -n "$time_range" ]]; then
        full_url="${full_url}&time=${time_range}"
    fi

    # Execute query with retries
    local max_retries=3
    local retry=0
    local response
    local curl_exit_code

    log_info "Querying Prometheus: $query"

    while [[ $retry -lt $max_retries ]]; do
        if [[ -n "$PROMETHEUS_TOKEN" ]]; then
            response=$(curl -sk -H "Authorization: Bearer $PROMETHEUS_TOKEN" \
                "$full_url" 2>&1)
            curl_exit_code=$?
        else
            response=$(curl -sk "$full_url" 2>&1)
            curl_exit_code=$?
        fi

        # Check curl exit code
        if [[ $curl_exit_code -ne 0 ]]; then
            log_info "  Attempt $((retry+1))/$max_retries: curl failed (exit code: $curl_exit_code)"
            ((retry++))
            if [[ $retry -lt $max_retries ]]; then
                sleep 2
            fi
            continue
        fi

        # Check if response is valid JSON and has data
        if echo "$response" | jq -e '.status == "success"' >/dev/null 2>&1; then
            log_info "  ✓ Query successful"
            echo "$response"
            return 0
        fi

        log_info "  Attempt $((retry+1))/$max_retries: Invalid response"
        ((retry++))
        if [[ $retry -lt $max_retries ]]; then
            sleep 2
        fi
    done

    # Query failed after retries - PANIC
    echo "" >&2
    echo "========================================================================" >&2
    echo "FATAL ERROR: Prometheus query failed after $max_retries attempts!" >&2
    echo "========================================================================" >&2
    echo "" >&2
    echo "Query: $query" >&2
    echo "Endpoint: $PROMETHEUS_ENDPOINT" >&2
    echo "" >&2
    echo "Full URL (for manual testing):" >&2
    echo "  $full_url" >&2
    echo "" >&2
    if [[ -n "$PROMETHEUS_TOKEN" ]]; then
        echo "Auth token: ${PROMETHEUS_TOKEN:0:20}..." >&2
        echo "" >&2
        echo "Manual test command:" >&2
        echo "  curl -sk -H 'Authorization: Bearer \$(oc whoami -t)' \\" >&2
        echo "    '$full_url'" >&2
    else
        echo "No auth token available!" >&2
        echo "" >&2
        echo "Manual test command:" >&2
        echo "  curl -sk '$full_url'" >&2
    fi
    echo "" >&2
    if [[ -n "$response" ]]; then
        echo "Last response:" >&2
        echo "$response" | head -20 >&2
        echo "" >&2
    fi
    echo "curl exit code: $curl_exit_code" >&2
    echo "" >&2
    exit 1
}

extract_metric_value() {
    local json=$1
    local metric_name=${2:-}
    local label_filter=${3:-}
    
    # Validate JSON input
    if [[ -z "$json" ]] || ! echo "$json" | jq -e . >/dev/null 2>&1; then
        echo "0"
        return
    fi
    
    # Extract scalar value from instant query result
    if [[ -z "$label_filter" ]]; then
        echo "$json" | jq -r '.data.result[0].value[1] // "0"' 2>/dev/null || echo "0"
    else
        # Filter by label and extract value
        echo "$json" | jq -r --arg filter "$label_filter" \
            '.data.result[] | select(.metric | contains($filter)) | .value[1] // "0"' 2>/dev/null || echo "0"
    fi
}

query_prometheus_range() {
    local query=$1
    local start=$2
    local end=$3
    local step=${4:-60}
    
    discover_prometheus_endpoint

    local api_path="/api/v1/query_range"
    local url="${PROMETHEUS_ENDPOINT}${api_path}"
    
    local encoded_query
    encoded_query=$(printf '%s' "$query" | jq -sRr @uri)
    
    local full_url="${url}?query=${encoded_query}&start=${start}&end=${end}&step=${step}"

    local response
    if [[ -n "$PROMETHEUS_TOKEN" ]]; then
        response=$(curl -sk -H "Authorization: Bearer $PROMETHEUS_TOKEN" \
            "$full_url" 2>/dev/null || echo "")
    else
        response=$(curl -sk "$full_url" 2>/dev/null || echo "")
    fi

    if echo "$response" | jq -e '.status == "success"' >/dev/null 2>&1; then
        echo "$response"
        return 0
    fi

    echo "ERROR: Prometheus range query failed" >&2
    echo "Query: $query" >&2
    exit 1
}

parse_volume_handle() {
    local handle=$1
    # volumeHandle format examples:
    # 0001-0024-openshift-storage-0000000000000001-abc123def456-pool-image
    # Extract pool and image from the end
    local pool image
    
    # Try to extract pool and image (last two segments after final cluster ID)
    if [[ "$handle" =~ -([^-]+)-([^-]+)$ ]]; then
        pool="${BASH_REMATCH[1]}"
        image="${BASH_REMATCH[2]}"
        echo "$pool/$image"
    else
        # Fallback: just return the image name if can't parse
        image=$(basename "$handle")
        echo "$image"
    fi
}

