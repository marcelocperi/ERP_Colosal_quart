import os
import mariadb
from database import DB_CONFIG

def test_inserts():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Probando inserción en stk_depositos...")
        cursor.execute("INSERT IGNORE INTO stk_depositos (id, enterprise_id, nombre, es_principal) VALUES (1, 1, 'Depósito Central', 1)")
        print(f"Filas afectadas: {cursor.rowcount}")
        
        print("Probando inserción en stk_motivos...")
        motivos = [
            (1, 1, 'Venta (Facturación)', 'SALIDA', 1),
            (2, 1, 'Compra (Recepción)', 'ENTRADA', 1),
            (3, 1, 'Devolución de Cliente', 'ENTRADA', 1),
            (4, 1, 'Ajuste de Inventario (+)', 'ENTRADA', 0),
            (5, 1, 'Ajuste de Inventario (-)', 'SALIDA', 0),
            (6, 1, 'Transferencia entre Depósitos', 'TRANSFERENCIA', 0)
        ]
        for m in motivos:
            try:
                cursor.execute("INSERT IGNORE INTO stk_motivos (id, enterprise_id, nombre, tipo, automatico) VALUES (%s, %s, %s, %s, %s)", m)
            except mariadb.Error as e:
                print(f"Error insertando motivo {m[0]}: {e}")
        
        conn.commit()
        print("✅ Fin del test.")
        
    except mariadb.Error as e:
        print(f"❌ Error MariaDB: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    test_inserts()
