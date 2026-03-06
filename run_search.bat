@echo off
python search_providers_no_code.py > providers_report.txt 2>&1
type providers_report.txt
