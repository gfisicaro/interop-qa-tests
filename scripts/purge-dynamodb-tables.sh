#!/bin/bash
set -eo pipefail

# Check dependencies
for cmd in aws jq; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "ERROR: $cmd is not installed" >&2
        exit 1
    fi
done

delete_item() {
    local table="$1"
    local key="$2"
    local retry=0
    local max_retries=3
    
    while (( retry <= max_retries )); do
        if aws dynamodb delete-item \
            --table-name "$table" \
            --key "$key" \
            --output text \
            --query '""'; then
            return 0
        else
            sleep $((2 ** retry))
            ((retry++))
        fi
    done
    echo "ERROR: Failed to delete item after $max_retries attempts" >&2
    return 1
}

purge_table() {
    local table_name="$1"
    local start_time=$(date +%s)
    local timeout_seconds=600  # 10 minutes
    
    echo "START - Purging table: $table_name"
    
    if ! aws dynamodb describe-table --table-name "$table_name" &> /dev/null; then
        echo "ERROR: Table $table_name does not exist" >&2
        return 1
    fi
    
    local last_evaluated_key=""
    local page_count=0
    
    while true; do
        if (( $(date +%s) - start_time > timeout_seconds )); then
            echo "ERROR: Timeout reached after $timeout_seconds seconds" >&2
            exit 1
        fi
        
        echo "Scanning table (page $((++page_count)))..."
        
        local scan_cmd=(aws dynamodb scan --table-name "$table_name" --output json)
        [[ -n "$last_evaluated_key" ]] && scan_cmd+=(--exclusive-start-key "$last_evaluated_key")
        
        local scan_result=$("${scan_cmd[@]}")
        local item_count=$(jq '.Count' <<< "$scan_result")
        
        (( item_count == 0 )) && break
        
        echo "Found $item_count items. Deleting..."
        
        jq -c '.Items[]' <<< "$scan_result" | \
        xargs -P 5 -I {} bash -c '
            key=$(jq '\''with_entries({
                key: .key,
                value: { S: .value.S, N: .value.N, B: .value.B }
            } | select(.value != {}))'\'' <<< "{}")
            delete_item "$0" "$key"
        ' "$table_name"
        
        last_evaluated_key=$(jq -c '.LastEvaluatedKey // empty' <<< "$scan_result")
        [[ -z "$last_evaluated_key" ]] && break
    done
    
    echo "DONE - Table $table_name has been purged."
}

# Main
tables=("$@")
for table in "${tables[@]}"; do
    purge_table "$table"
done