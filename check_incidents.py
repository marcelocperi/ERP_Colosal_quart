import os
import sys

# Agregar la ruta base al sys.path para poder importar app y db
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

from app import app, db
from sqlalchemy import text
import pprint

with app.app_context():
    result = db.session.execute(text('SELECT id, user_id, timestamp, log_level, source, message_type, message_summary, status_code, endpoint, error_traceback FROM sys_transaction_logs ORDER BY id DESC LIMIT 5;')).mappings().all()
    print("----- ULTIMOS INCIDENTES (LOGS) -----")
    for row in result:
        print("------------------------------------------")
        for key, value in row.items():
            print(f"{key}: {value}")

