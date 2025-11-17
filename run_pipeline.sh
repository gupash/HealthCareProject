
# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Error: Virtual environment is not activated"
    echo "Please run: source venv/bin/activate"
    exit 1
fi

echo "--- All checks passed. Running the ingest stream ---"
python src/ingest_stream.py

echo "--- Pipeline execution finished successfully ---"