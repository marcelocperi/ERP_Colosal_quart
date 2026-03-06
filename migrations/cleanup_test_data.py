import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def clean_system():
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Get all tables
        cursor.execute("SHOW TABLES")
        table_key = 'Tables_in_erp' # This might change depending on the DB name
        raw_tables = cursor.fetchall()
        
        # Detect the key name for the table list
        if raw_tables:
            keys = list(raw_tables[0].keys())
            table_key = keys[0]
        
        tables = [row[table_key] for row in raw_tables]
        
        tables_with_ent = []
        for table in tables:
            if table == 'sys_enterprises':
                continue
            try:
                cursor.execute(f"SHOW COLUMNS FROM `{table}`")
                columns = [c['Field'] for c in cursor.fetchall()]
                if 'enterprise_id' in columns:
                    tables_with_ent.append(table)
            except:
                # Ignore views or tables without permissions
                continue

    print(f"Borrando registros de Empresa 1 y 4 en {len(tables_with_ent)} tablas...")
    
    with get_db_cursor(dictionary=True) as cursor:
        # Deshabilitar FK checks para limpieza masiva
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # 2. Delete from tables with enterprise_id
        for table in tables_with_ent:
            try:
                cursor.execute(f"DELETE FROM `{table}` WHERE enterprise_id IN (1, 4)")
                count = cursor.rowcount
                if count > 0:
                    print(f"  - {table}: {count} filas borradas.")
            except Exception as e:
                print(f"  - Error en {table}: {str(e)}")

        # 3. Cleanup specialized orphans (parents might have been IDs 1, 4)
        # Check erp_comprobantes_detalle
        cursor.execute("DELETE FROM erp_comprobantes_detalle WHERE comprobante_id NOT IN (SELECT id FROM erp_comprobantes)")
        if cursor.rowcount > 0:
            print(f"  - erp_comprobantes_detalle (huérfanos): {cursor.rowcount} filas.")

        # Check cont_asientos_detalle
        cursor.execute("DELETE FROM cont_asientos_detalle WHERE asiento_id NOT IN (SELECT id FROM cont_asientos)")
        if cursor.rowcount > 0:
            print(f"  - cont_asientos_detalle (huérfanos): {cursor.rowcount} filas.")

        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        # 4. Cleanup other common child tables
        child_tables = ['erp_movimientos_fondos', 'cont_asientos_detalle', 'stk_movimientos']
        for table in child_tables:
            # Note: most of these ALREADY have enterprise_id, but double checking for cross-table orphans
            pass

    print("\nLimpieza completada.")

if __name__ == '__main__':
    clean_system()
