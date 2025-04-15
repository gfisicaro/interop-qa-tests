#!/bin/bash
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:8000}"
export AWS_PROFILE="${AWS_PROFILE:-default}"
TABLE_NAME="$1"
REGION="${2:-us-east-1}"

BATCH_SIZE=25
MAX_RETRIES=3
SLEEP_TIME=1
MAX_CYCLES=20
STUBBORN_RETRIES=5
MAX_PARALLEL_PROCESSES=4
PROGRESS_BAR_WIDTH=50

if [[ -z "$TABLE_NAME" ]]; then
  echo "Usage: $0 <table-name> [region]"
  exit 1
fi

echo "Purging table '$TABLE_NAME' in region '$REGION'..."

# Start timing
START_TIME=$(date +%s)

# Function to draw progress bar
draw_progress_bar() {
  local current=$1
  local total=$2
  local cycle=$3
  local max_cycles=$4
  
  if [[ $total -eq 0 ]]; then
    return
  fi
  
  local percent=$((current * 100 / total))
  local progress=$((current * PROGRESS_BAR_WIDTH / total))
  local remaining=$((PROGRESS_BAR_WIDTH - progress))
  
  printf "\rCycle %2d/%d: [" "$cycle" "$max_cycles"
  printf "%${progress}s" | tr ' ' '='
  printf "%${remaining}s" | tr ' ' ' '
  printf "] %3d%% (%d/%d items)" "$percent" "$current" "$total"
}

# Get table schema
KEYS=$(aws dynamodb describe-table \
  --table-name "$TABLE_NAME" \
  --region "$REGION" \
  --query "Table.KeySchema" \
  --output json)

HASH_KEY=$(echo "$KEYS" | jq -r '.[] | select(.KeyType=="HASH") | .AttributeName')
RANGE_KEY=$(echo "$KEYS" | jq -r '.[] | select(.KeyType=="RANGE") | .AttributeName')

echo "Primary key: $HASH_KEY"
[[ -n "$RANGE_KEY" && "$RANGE_KEY" != "null" ]] && echo "Sort key: $RANGE_KEY"

get_item_count() {
  aws dynamodb describe-table \
    --table-name "$TABLE_NAME" \
    --region "$REGION" \
    --query "Table.ItemCount" \
    --output text
}

process_batch_parallel() {
  local BATCH_ITEMS=$1
  local BATCH_REQUEST=$(jq -n '{ "$TABLE_NAME": [] }' | \
    jq --argjson items "$BATCH_ITEMS" --arg table "$TABLE_NAME" \
    '{ ($table): $items }')
  
  local RETRY=0
  while [[ $RETRY -lt $MAX_RETRIES ]]; do
    if aws dynamodb batch-write-item \
      --request-items "$BATCH_REQUEST" \
      --region "$REGION" >/dev/null; then
      return 0
    fi
    RETRY=$((RETRY + 1))
    echo -e "\nBatch delete failed (attempt $RETRY/$MAX_RETRIES), retrying..."
    sleep 1
  done
  echo -e "\nError: Failed to delete batch after $MAX_RETRIES attempts"
  return 1
}

delete_single_item_parallel() {
  local ITEM=$1
  local HASH_VALUE=$(echo "$ITEM" | jq ".[\"$HASH_KEY\"]")
  local RANGE_VALUE=""
  
  if [[ -n "$RANGE_KEY" && "$RANGE_KEY" != "null" ]]; then
    RANGE_VALUE=$(echo "$ITEM" | jq ".[\"$RANGE_KEY\"]")
  fi
  
  local KEY_JSON
  if [[ -n "$RANGE_VALUE" && "$RANGE_VALUE" != "null" ]]; then
    KEY_JSON="{\"$HASH_KEY\": $HASH_VALUE, \"$RANGE_KEY\": $RANGE_VALUE}"
  else
    KEY_JSON="{\"$HASH_KEY\": $HASH_VALUE}"
  fi
  
  local RETRY=0
  while [[ $RETRY -lt $STUBBORN_RETRIES ]]; do
    if aws dynamodb delete-item \
      --table-name "$TABLE_NAME" \
      --region "$REGION" \
      --key "$KEY_JSON" >/dev/null; then
      return 0
    fi
    RETRY=$((RETRY + 1))
    echo -e "\nStubborn item delete failed (attempt $RETRY/$STUBBORN_RETRIES), retrying..."
    sleep 2
  done
  echo -e "\nError: Failed to delete stubborn item after $STUBBORN_RETRIES attempts"
  return 1
}

purge_table() {
  local TOTAL_DELETED=0
  local CYCLES=0
  local LAST_REMAINING_ITEM=""
  local LAST_REMAINING_COUNT=0
  
  while [[ $CYCLES -lt $MAX_CYCLES ]]; do
    local ITEM_COUNT=$(get_item_count)
    [[ $ITEM_COUNT -eq 0 ]] && break
    
    # Stubborn item handling
    if [[ $ITEM_COUNT -eq 1 && "$LAST_REMAINING_ITEM" != "" && $LAST_REMAINING_COUNT -ge 2 ]]; then
      echo -e "\nDetected stubborn item that won't delete. Trying special handling..."
      
      if delete_single_item_parallel "$LAST_REMAINING_ITEM"; then
        TOTAL_DELETED=$((TOTAL_DELETED + 1))
        echo -e "\n✅ Finally deleted stubborn item"
        LAST_REMAINING_ITEM=""
        LAST_REMAINING_COUNT=0
        continue
      else
        echo -e "\n⚠️ Could not delete stubborn item. It may be recreating itself or have permission issues."
        return 1
      fi
    fi
    
    echo -e "\nCycle $((CYCLES + 1))/$MAX_CYCLES - Items remaining: $ITEM_COUNT"
    local LAST_EVALUATED_KEY="init"
    local ITEMS_TO_DELETE=()
    
    # Scan phase - collect items to delete
    while [[ "$LAST_EVALUATED_KEY" != "null" ]]; do
      if [[ "$LAST_EVALUATED_KEY" == "init" ]]; then
        SCAN_RESULT=$(aws dynamodb scan \
          --table-name "$TABLE_NAME" \
          --region "$REGION" \
          --output json)
      else
        SCAN_RESULT=$(aws dynamodb scan \
          --table-name "$TABLE_NAME" \
          --region "$REGION" \
          --exclusive-start-key "$LAST_EVALUATED_KEY" \
          --output json)
      fi
      
      # Store last item for stubborn handling
      if [[ $(echo "$SCAN_RESULT" | jq '.Items | length') -gt 0 ]]; then
        LAST_REMAINING_ITEM=$(echo "$SCAN_RESULT" | jq -c '.Items[-1]')
      fi
      
      # Add all items to deletion array
      while read -r item; do
        ITEMS_TO_DELETE+=("$item")
        # Update progress during scan
        draw_progress_bar ${#ITEMS_TO_DELETE[@]} "$ITEM_COUNT" "$((CYCLES + 1))" "$MAX_CYCLES"
      done < <(echo "$SCAN_RESULT" | jq -c '.Items[]')
      
      LAST_EVALUATED_KEY=$(echo "$SCAN_RESULT" | jq -c '.LastEvaluatedKey // null')
    done
    
    # Delete phase - process in parallel
    if [[ ${#ITEMS_TO_DELETE[@]} -gt 0 ]]; then
      local NUM_BATCHES=$(( (${#ITEMS_TO_DELETE[@]} + BATCH_SIZE - 1) / BATCH_SIZE ))
      local BATCHES_PROCESSED=0
      local DELETED_IN_CYCLE=0
      
      # Use a temporary file to track progress from subshells
      PROGRESS_FILE=$(mktemp)
      echo "0" > "$PROGRESS_FILE"
      
      for ((i=0; i<NUM_BATCHES; i++)); do
        (
          local BATCH_START=$((i * BATCH_SIZE))
          local BATCH_END=$(( (i + 1) * BATCH_SIZE ))
          local BATCH_ITEMS="[]"
          
          for ((j=BATCH_START; j<BATCH_END && j<${#ITEMS_TO_DELETE[@]}; j++)); do
            local ITEM="${ITEMS_TO_DELETE[$j]}"
            local HASH_VALUE=$(echo "$ITEM" | jq ".[\"$HASH_KEY\"]")
            local DELETE_REQUEST=$(jq -n '{ DeleteRequest: { Key: {} } }')
            DELETE_REQUEST=$(echo "$DELETE_REQUEST" | jq --argjson val "$HASH_VALUE" ".DeleteRequest.Key[\"$HASH_KEY\"] = \$val")
            
            if [[ -n "$RANGE_KEY" && "$RANGE_KEY" != "null" ]]; then
              local RANGE_VALUE=$(echo "$ITEM" | jq ".[\"$RANGE_KEY\"]")
              DELETE_REQUEST=$(echo "$DELETE_REQUEST" | jq --argjson val "$RANGE_VALUE" ".DeleteRequest.Key[\"$RANGE_KEY\"] = \$val")
            fi
            
            BATCH_ITEMS=$(echo "$BATCH_ITEMS" | jq --argjson req "$DELETE_REQUEST" '. += [$req]')
          done
          
          if [[ $(echo "$BATCH_ITEMS" | jq 'length') -gt 0 ]]; then
            if process_batch_parallel "$BATCH_ITEMS"; then
              # Update progress
              flock -x "$PROGRESS_FILE" -c "echo \$(( \$(cat "$PROGRESS_FILE") + $(echo "$BATCH_ITEMS" | jq 'length') )) > "$PROGRESS_FILE""
            else
              exit 1
            fi
          fi
        ) &
        
        # Limit parallel processes
        if [[ $(jobs -r -p | wc -l) -ge $MAX_PARALLEL_PROCESSES ]]; then
          wait -n
        fi
        
        # Update progress bar
        DELETED_IN_CYCLE=$(<"$PROGRESS_FILE")
        draw_progress_bar "$DELETED_IN_CYCLE" "$ITEM_COUNT" "$((CYCLES + 1))" "$MAX_CYCLES"
      done
      
      # Wait for all jobs to complete
      wait
      rm -f "$PROGRESS_FILE"
      
      TOTAL_DELETED=$((TOTAL_DELETED + ${#ITEMS_TO_DELETE[@]}))
      echo -e "\nDeleted ${#ITEMS_TO_DELETE[@]} items (total: $TOTAL_DELETED)"
    fi
    
    # Check if we're stuck with the same count
    local NEW_ITEM_COUNT=$(get_item_count)
    if [[ $NEW_ITEM_COUNT -eq $ITEM_COUNT ]]; then
      LAST_REMAINING_COUNT=$((LAST_REMAINING_COUNT + 1))
    else
      LAST_REMAINING_COUNT=0
    fi
    
    CYCLES=$((CYCLES + 1))
    [[ $NEW_ITEM_COUNT -eq 0 ]] && break
    echo "Waiting $SLEEP_TIME second(s) before next cycle..."
    sleep $SLEEP_TIME
  done
  
  if [[ $CYCLES -ge $MAX_CYCLES ]]; then
    echo -e "\nWarning: Reached maximum cycles ($MAX_CYCLES) before completing purge"
    echo "Deleted $TOTAL_DELETED items total. Last remaining item may be recreating itself."
    return 1
  fi
  
  # Calculate elapsed time
  END_TIME=$(date +%s)
  ELAPSED_TIME=$((END_TIME - START_TIME))
  
  echo -e "\n✅ Table '$TABLE_NAME' completely purged. Total items deleted: $TOTAL_DELETED"
  echo "Total time elapsed: $ELAPSED_TIME seconds"
  return 0
}

if ! purge_table; then
  END_TIME=$(date +%s)
  ELAPSED_TIME=$((END_TIME - START_TIME))
  echo "Operation took $ELAPSED_TIME seconds before exiting"
  exit 1
fi

echo "Operation completed successfully"