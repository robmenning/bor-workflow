#!/bin/bash
set -e

if [ ! -d "venv" ]; then
  python3 -m venv venv
fi

source venv/bin/activate
# pip install -r requirements.txt # not always necessary

# pip install pandas numpy scipy
# python3 tests/gen-bottom-holdings.py


python src/workflows/pdf_extraction.py



deactivate
