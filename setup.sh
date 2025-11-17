#!/bin/bash

set -e

echo "--- Setting up the project ---"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

echo "--- Creating virtual environment ---"
python3 -m venv venv

echo "--- Activating virtual environment ---"
source venv/bin/activate

echo "--- Installing dependencies ---"
pip install --upgrade pip
pip install -r requirements.txt

echo "--- Setup complete! ---"
echo "To activate the virtual environment, run: source venv/bin/activate"
