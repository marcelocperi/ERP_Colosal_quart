import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def upgrade_printers_table():
    print("Actualizando tabla stk_impresoras_config para soporte de 3 modos de conexión...")
    try:
        with get_db_cursor() as cursor:
            # Add connection type
            try:
                cursor.execute("""
                    ALTER TABLE stk_impresoras_config 
                    ADD COLUMN tipo_conexion ENUM('BROWSER_DIALOG','IP_RED','QZ_TRAY') DEFAULT 'BROWSER_DIALOG' AFTER activo
                """)
                print("Columna tipo_conexion agregada.")
            except Exception as e:
                print(f"  (tipo_conexion ya existe o error: {e})")

            # Add IP field for network printers
            try:
                cursor.execute("""
                    ALTER TABLE stk_impresoras_config 
                    ADD COLUMN ip_red VARCHAR(45) DEFAULT NULL AFTER tipo_conexion
                """)
                print("Columna ip_red agregada.")
            except Exception as e:
                print(f"  (ip_red ya existe o error: {e})")

            # Add port for ZPL/RAW (default 9100 for Zebra)
            try:
                cursor.execute("""
                    ALTER TABLE stk_impresoras_config 
                    ADD COLUMN puerto_red INT DEFAULT 9100 AFTER ip_red
                """)
                print("Columna puerto_red agregada.")
            except Exception as e:
                print(f"  (puerto_red ya existe o error: {e})")

            # Add QZ Tray printer name (as registered in OS)
            try:
                cursor.execute("""
                    ALTER TABLE stk_impresoras_config 
                    ADD COLUMN nombre_sistema_qz VARCHAR(100) DEFAULT NULL AFTER puerto_red
                """)
                print("Columna nombre_sistema_qz agregada.")
            except Exception as e:
                print(f"  (nombre_sistema_qz ya existe o error: {e})")

        print("Upgrade completado.")
    except Exception as e:
        print(f"Error en upgrade: {e}")

if __name__ == "__main__":
    upgrade_printers_table()
