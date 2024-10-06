#!/bin/bash
source ../.venv/bin/activate
python3.11 simulate_sales.py
python3.11 ingest_csv_to_postgress.py

deactivate
