import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db_cursor

with get_db_cursor() as cursor:
    cursor.execute("ALTER TABLE sys_transaction_logs MODIFY endpoint VARCHAR(500);")
    cursor.execute("ALTER TABLE sys_transaction_logs MODIFY module VARCHAR(255);")
    cursor.execute("ALTER TABLE sys_transaction_logs MODIFY failure_mode VARCHAR(255);")
    cursor.execute("ALTER TABLE sys_transaction_logs MODIFY request_method VARCHAR(50);")
    print("Columnas de sys_transaction_logs ampliadas exitosamente.")
