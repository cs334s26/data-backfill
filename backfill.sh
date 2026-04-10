#!/bin/bash
# run_backfill.sh
#
# Sets up the environment and runs the full backfill for a range of agencies
# in the background. Logs all output to a file.
#
# Usage:
#   chmod +x run_backfill.sh
#   ./run_backfill.sh <start> [end]
#
# Examples:
#   ./run_backfill.sh 1 15     # process agencies 1 through 15
#   ./run_backfill.sh 1 315    # process all 315 agencies
#   ./run_backfill.sh 1        # process agency 1 only

set -e

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
if [ -z "$1" ]; then
    echo "Usage: ./run_backfill.sh <start> [end] [--since YYYY-MM-DD]"
    echo ""
    echo "  start           Agency index to start from (1-based)"
    echo "  end             Agency index to stop at, inclusive (default: same as start)"
    echo "  --since DATE    Only process documents posted on or after this date"
    echo ""
    echo "  Examples:"
    echo "    ./run_backfill.sh 1 315                        # all agencies"
    echo "    ./run_backfill.sh 1 315 --since 2026-04-01    # only new documents"
    exit 1
fi

START=$1
END=${2:-$1}
SINCE=""

# Check for --since flag
for i in "$@"; do
    if [ "$i" = "--since" ]; then
        SINCE_NEXT=1
    elif [ -n "$SINCE_NEXT" ]; then
        SINCE="--since $i"
        unset SINCE_NEXT
    fi
done

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/backfill_${START}_${END}.log"
VENV="$SCRIPT_DIR/.venv"

echo "=========================================="
echo " Regulations Backfill"
echo " Agencies: $START to $END"
echo " Log file: $LOG_FILE"
echo "=========================================="

# Load environment variables
if [ -f ~/.env ]; then
    source ~/.env
    echo "Loaded environment from ~/.env"
else
    echo "Warning: ~/.env not found — make sure OPENSEARCH_HOST and AWS_REGION are set"
fi

# Activate virtual environment if it exists
if [ -d "$VENV" ]; then
    source "$VENV/bin/activate"
    echo "Activated virtual environment: $VENV"
fi

# Install dependencies if needed
echo "Checking dependencies..."
pip3 install -q requests beautifulsoup4 opensearch-py boto3 urllib3

# ---------------------------------------------------------------------------
# Run in background
# ---------------------------------------------------------------------------
echo "Starting backfill in background..."
echo "To monitor progress: tail -f $LOG_FILE"
echo "To check if running: ps aux | grep ingest_regulations"
echo ""

nohup python3 "$SCRIPT_DIR/ingest_regulations.py" "$START" "$END" $SINCE \
    > "$LOG_FILE" 2>&1 &

PID=$!
echo "Started with PID: $PID"
echo "PID saved to: $SCRIPT_DIR/backfill_${START}_${END}.pid"
echo $PID > "$SCRIPT_DIR/backfill_${START}_${END}.pid"

echo ""
echo "The backfill is now running in the background."
echo "You can safely close this terminal."
echo ""
echo "To monitor:  tail -f $LOG_FILE"
echo "To stop:     kill \$(cat $SCRIPT_DIR/backfill_${START}_${END}.pid)"