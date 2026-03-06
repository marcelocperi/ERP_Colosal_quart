
import mariadb
from database import DB_CONFIG

def check_stk():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Checking stk_existencias...")
        cursor.execute("SELECT 1 FROM stk_existencias LIMIT 1")
        print("stk_existencias OK")
    except Exception as e:
        print(f"stk_existencias ERROR: {e}")

    try:
        cursor.execute("SELECT 1 FROM stk_movimientos LIMIT 1")
        print("stk_movimientos OK")
    except Exception as e:
        print(f"stk_movimientos ERROR: {e}")

    conn.close()

if __name__ == "__main__":
    check_stk()
