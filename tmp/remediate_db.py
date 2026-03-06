
from database import get_db_cursor

def remediate_database():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_TYPE = 'BASE TABLE'
        """)
        tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
        
        print("INICIANDO REMEDIACIÓN DE ESQUEMAS - FASE 1 (BASE DE DATOS)")
        print("="*60)
        
        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            columns = {row['Field'].lower(): row for row in cursor.fetchall()}
            
            alterations = []
            
            # 1. Asegurar campos de CREACIÓN
            if not any(c in columns for c in ['user_id', 'usuario_id', 'created_by']):
                alterations.append("ADD COLUMN user_id INT NULL DEFAULT NULL COMMENT 'ID Usuario Creador (Audit)'")
            
            if not any(c in columns for c in ['created_at', 'fecha_alta', 'dt_date_create']):
                alterations.append("ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Fecha Creación (Audit)'")
                
            # 2. Asegurar campos de ACTUALIZACIÓN (La brecha más grande según el informe)
            if not any(c in columns for c in ['updated_at', 'dt_date_update', 'fecha_mod']):
                alterations.append("ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Fecha Modificación (Audit)'")
            
            if not any(c in columns for c in ['user_id_update', 'updated_by', 'usuario_mod']):
                alterations.append("ADD COLUMN user_id_update INT NULL DEFAULT NULL COMMENT 'ID Usuario Modificador (Audit)'")

            if alterations:
                sql = f"ALTER TABLE {table} " + ", ".join(alterations)
                try:
                    cursor.execute(sql)
                    print(f"✅ {table:<35} | Modificada: {len(alterations)} campos añadidos.")
                except Exception as e:
                    print(f"❌ {table:<35} | Error: {e}")
            else:
                print(f"➖ {table:<35} | Ya cumple con el estándar de trazabilidad.")

if __name__ == "__main__":
    remediate_database()
