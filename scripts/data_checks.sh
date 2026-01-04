#!/usr/bin/env bash

# Quick data checks using grep, awk, sed before ingestion

RAW_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../raw" && pwd)"

CSV_FILE="$RAW_DIR/transport.csv"

# 1. Show header and first 5 rows
head -n 6 "$CSV_FILE"

# 2. Check for missing ridership values
awk -F, 'NR>1 && ($3 == "" || $3 == "NA") {print "Missing ridership:", $0}' "$CSV_FILE"

# 3. Validate timestamp format (very basic ISO-8601 check)
grep -nE '^[0-9]+,[^,]+,[0-9]+,[0-9]{4}-[0-9]{2}-[0-9]{2}T' "$CSV_FILE" | head

# 4. Simple sed example: trim spaces around commas (if any)
sed -E 's/ *, */,/g' "$CSV_FILE" | head -n 5
