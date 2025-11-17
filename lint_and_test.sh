#!/bin/bash

set -e

echo "--- Auto-fixing with Ruff ---"
ruff check --fix .

echo "--- Auto-formatting with Ruff ---"
ruff format .

echo "Running mypy static type checker..."
mypy "src/"

echo "--- Running Pytest for validation ---"
pytest

echo "--- Done. Please review changes before committing. ---"