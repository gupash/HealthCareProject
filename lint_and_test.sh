#!/bin/bash

set -e

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Error: Virtual environment is not activated"
    echo "Please run: source venv/bin/activate"
    exit 1
fi

echo "--- Auto-fixing with Ruff ---"
ruff check --fix .

echo "--- Auto-formatting with Ruff ---"
ruff format .

echo "Running mypy static type checker..."
mypy "src/"

echo "--- Running Pytest for validation ---"
pytest

echo "--- Done. Please review changes before committing. ---"