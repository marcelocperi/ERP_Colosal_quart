
from database import get_db_cursor
def check_metadata():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM sys_tipos_comprobante LIMIT 1")
        row = cursor.fetchone()
        if row:
            for k in row.keys():
                print(f"Col: {k}")
            
        cursor.execute("SELECT * FROM sys_tipos_comprobante")
        print("\nTypes:")
        for r in cursor.fetchall():
            print(f"  {r.get('codigo')} - {r.get('descripcion') or r.get('nombre')}")

        cursor.execute("SELECT * FROM stk_motivos")
        print("\nMotivos:")
        for r in cursor.fetchall():
            print(f"  {r.get('id')} - {r.get('nombre')} ({r.get('tipo')})")

if __name__ == '__main__':
    check_metadata()
