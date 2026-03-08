#!/usr/bin/env bash
# =============================================================================
# TTB Wine Production Pipeline — Full Run Script
# =============================================================================
#
# This script runs all 4 stages of the pipeline in sequence:
#   1. Download  — fetch source files from TTB (with ETag caching)
#   2. Parse     — normalize raw data into clean tables
#   3. Transform — compute derived metrics (YoY changes, ratios, etc.)
#   4. Export    — generate summary.json and validate outputs
#
# Usage:
#   bash scripts/run_pipeline.sh          # normal run (skip unchanged files)
#   bash scripts/run_pipeline.sh --force  # force re-download all files
#
# The pipeline is idempotent: running it multiple times produces the same
# output. The download stage uses ETag/Last-Modified caching to avoid
# re-downloading unchanged files.
# =============================================================================

set -euo pipefail

# Navigate to project root (parent of scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "============================================================"
echo "TTB Wine Production Pipeline"
echo "============================================================"
echo "Project root: $PROJECT_ROOT"
echo "Started at:   $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo ""

# Check Python is available
if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 is required but not found on PATH"
    exit 1
fi

# Check required packages are installed
python3 -c "import pandas, requests, openpyxl" 2>/dev/null || {
    echo "Installing required packages..."
    pip install -q -r requirements.txt
}

# Parse arguments
FORCE_FLAG=""
if [[ "${1:-}" == "--force" ]]; then
    FORCE_FLAG="--force"
    echo "Mode: FORCE (re-downloading all files)"
else
    echo "Mode: INCREMENTAL (skipping unchanged files)"
fi
echo ""

# ---------------------------------------------------------------------------
# Stage 1: Download source files from TTB
# ---------------------------------------------------------------------------
echo ">>> Stage 1: Download"
python3 -c "
import sys
sys.path.insert(0, '.')
from pipeline.download import run
run(force='$FORCE_FLAG' == '--force')
"
echo ""

# ---------------------------------------------------------------------------
# Stage 2: Parse and normalize raw data
# ---------------------------------------------------------------------------
echo ">>> Stage 2: Parse"
python3 -c "
import sys
sys.path.insert(0, '.')
from pipeline.parse import run
run()
"
echo ""

# ---------------------------------------------------------------------------
# Stage 3: Compute derived metrics
# ---------------------------------------------------------------------------
echo ">>> Stage 3: Transform"
python3 -c "
import sys
sys.path.insert(0, '.')
from pipeline.transform import run
run()
"
echo ""

# ---------------------------------------------------------------------------
# Stage 4: Export summary and validate
# ---------------------------------------------------------------------------
echo ">>> Stage 4: Export"
python3 -c "
import sys
sys.path.insert(0, '.')
from pipeline.export import run
run()
"
echo ""

echo "============================================================"
echo "Pipeline complete at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Output files in: data/processed/"
echo "============================================================"
ls -lh data/processed/
