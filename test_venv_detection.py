import os
import sys

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
parent_venv = os.path.abspath(os.path.join(PROJECT_DIR, "..", "venv"))
local_venv = os.path.join(PROJECT_DIR, "venv")

print(f"PROJECT_DIR: {PROJECT_DIR}")
print(f"parent_venv: {parent_venv}")
print(f"parent_venv exists: {os.path.exists(parent_venv)}")
print(f"local_venv: {local_venv}")
print(f"local_venv exists: {os.path.exists(local_venv)}")

if os.path.exists(parent_venv):
    VENV_DIR = parent_venv
elif os.path.exists(local_venv):
    VENV_DIR = local_venv
else:
    VENV_DIR = "NOT FOUND"

print(f"Final VENV_DIR: {VENV_DIR}")
PYTHON_EXE = os.path.join(VENV_DIR, "Scripts", "python.exe")
print(f"Final PYTHON_EXE: {PYTHON_EXE}")
print(f"PYTHON_EXE exists: {os.path.exists(PYTHON_EXE)}")
