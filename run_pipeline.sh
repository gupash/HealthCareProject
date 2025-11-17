
# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Error: Virtual environment is not activated"
    echo "Please run: source venv/bin/activate"
    exit 1
fi

# Ensure we are at project root (directory containing src/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Add src to PYTHONPATH for this invocation
export PYTHONPATH="$PWD/src:${PYTHONPATH:-}"

echo "--- Running ingest stream ---"
python -m healthcare.ingest_stream
echo "--- Pipeline execution finished successfully ---"