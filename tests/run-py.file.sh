python3 -m venv venv
source venv/bin/activate
pip install pandas numpy scipy
python3 tests/gen-bottom-holdings.py
deactivate
