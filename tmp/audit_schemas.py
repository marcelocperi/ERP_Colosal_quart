
from database import get_db_cursor

def audit_tables_schema():
    with get_db_cursor(dictionary=True) as cursor:
        # Buscamos todas las tablas de negocio (transaccionales)
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE()
            AND (TABLE_NAME LIKE 'fin_%' 
                 OR TABLE_NAME LIKE 'cmp_%' 
                 OR TABLE_NAME LIKE 'stk_%' 
                 OR TABLE_NAME LIKE 'ven_%'
                 OR TABLE_NAME LIKE 'vta_%')
        """)
        tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
        
        print(f"{'TABLA':<35} | {'USER/CREATE':<15} | {'USER/UPDATE':<15} | {'ESTADO'}")
        print("-" * 85)
        
        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            columns = {row['Field'].lower(): row for row in cursor.fetchall()}
            
            # Check Create
            has_user_create = any(c in columns for c in ['user_id', 'usuario_id', 'created_by'])
            has_date_create = any(c in columns for c in ['created_at', 'fecha_alta', 'dt_date_create'])
            
            # Check Update
            has_user_update = any(c in columns for c in ['updated_by', 'usuario_mod', 'user_update_id'])
            has_date_update = any(c in columns for c in ['updated_at', 'fecha_mod', 'dt_date_update'])
            
            create_status = "✅ OK" if (has_user_create or has_date_create) else "❌ FALTA"
            update_status = "✅ OK" if (has_user_update or has_date_update) else "⚠️ PARCIAL"
            
            overall = "ROBUSTA" if (has_user_create and has_date_create) else "AUDITABLE"
            if not has_user_create and not has_date_create:
                overall = "CRÍTICA"
            
            print(f"{table:<35} | {create_status:<15} | {update_status:<15} | {overall}")

if __name__ == "__main__":
    audit_tables_schema()
