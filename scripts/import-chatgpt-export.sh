#!/usr/bin/env bash
#
# Import ChatGPT web export into cass-indexable format.
#
# PROBLEM:
# ChatGPT Mac app encrypts conversations (v2/v3) with AES-256-GCM. The key is
# stored in macOS Keychain under access group '2DC432GLL2.com.openai.shared',
# which is protected by Apple's code signing. Only OpenAI-signed apps can access it.
#
# SOLUTION:
# ChatGPT web export (Settings → Data Controls → Export) produces 'conversations.json'
# with the same JSON structure. The connector treats directories without '-v2-' or '-v3-'
# in the name as unencrypted. This script splits the export into individual files.
#
# USAGE:
#   ./import-chatgpt-export.sh <conversations.json> [output-dir]
#
# After importing, run: cass index
#

set -euo pipefail

EXPORT_FILE="${1:-}"
OUTPUT_DIR="${2:-$HOME/Library/Application Support/com.openai.chat}"

if [[ -z "$EXPORT_FILE" ]]; then
    echo "Usage: $0 <conversations.json> [output-dir]" >&2
    echo "" >&2
    echo "Import ChatGPT web export into cass-indexable format." >&2
    echo "" >&2
    echo "Arguments:" >&2
    echo "  conversations.json  Path to ChatGPT web export file" >&2
    echo "  output-dir          Optional. Default: ~/Library/Application Support/com.openai.chat/" >&2
    exit 1
fi

if [[ ! -f "$EXPORT_FILE" ]]; then
    echo "Error: File not found: $EXPORT_FILE" >&2
    exit 1
fi

# Check for jq
if ! command -v jq &>/dev/null; then
    echo "Error: jq is required. Install with: brew install jq" >&2
    exit 1
fi

CONV_DIR="$OUTPUT_DIR/conversations-web-export"
mkdir -p "$CONV_DIR"

echo "Loading $EXPORT_FILE..."

# Count conversations
TOTAL=$(jq 'length' "$EXPORT_FILE")
echo "Found $TOTAL conversations"

IMPORTED=0
SKIPPED=0

# Process each conversation
for i in $(seq 0 $((TOTAL - 1))); do
    # Extract conversation ID (fallback to index)
    CONV_ID=$(jq -r ".[$i].id // .[$i].conversation_id // \"conv-$i\"" "$EXPORT_FILE")
    OUTFILE="$CONV_DIR/$CONV_ID.json"

    if [[ -f "$OUTFILE" ]]; then
        ((SKIPPED++)) || true
        continue
    fi

    jq ".[$i]" "$EXPORT_FILE" > "$OUTFILE"
    ((IMPORTED++)) || true

    if (( IMPORTED % 100 == 0 )); then
        echo "  Processed $IMPORTED..."
    fi
done

echo ""
echo "Import complete!"
echo "  Total conversations: $TOTAL"
echo "  Newly imported:      $IMPORTED"
echo "  Skipped (existing):  $SKIPPED"
echo "  Output directory:    $CONV_DIR"
echo ""
echo "Next step: Run 'cass index' to index the conversations."
