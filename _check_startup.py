import os
import sys
from app import app
from database import init_db

print("Checking DB connection...")
if init_db():
    print("DB connection OK")
else:
    print("DB connection FAILED")
    sys.exit(1)

print("Attempting to initialize app context...")
try:
    with app.app_context():
        print("App context OK")
except Exception as e:
    print(f"App context FAILED: {e}")
    sys.exit(1)

print("Startup check finished successfully.")
