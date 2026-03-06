
import mariadb
from database import DB_CONFIG

def update_schema():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if es_pendiente exists in stk_motivos
        cursor.execute("DESCRIBE stk_motivos")
        cols = [row[0] for row in cursor.fetchall()]
        
        if 'es_pendiente' not in cols:
            print("Adding es_pendiente to stk_motivos...")
            cursor.execute("ALTER TABLE stk_motivos ADD COLUMN es_pendiente TINYINT(1) DEFAULT 0")
        else:
            print("es_pendiente already exists in stk_motivos.")

        if 'system_code' not in cols:
             print("Adding system_code to stk_motivos...")
             cursor.execute("ALTER TABLE stk_motivos ADD COLUMN system_code VARCHAR(50)")
        else:
             print("system_code already exists in stk_motivos.")

        # Sync/Copy motives from old table to new if needed
        # Check for 'stock_motivos'

        cursor.execute("SHOW TABLES LIKE 'stock_motivos'")
        if cursor.fetchone():
             print("Migrating motives from stock_motivos to stk_motivos...")
             cursor = conn.cursor(dictionary=True) # Switch to dict cursor
             cursor.execute("SELECT * FROM stock_motivos")
             stock_motives = cursor.fetchall()
             
             cursor.execute("SELECT id, nombre, system_code FROM stk_motivos")
             existing_motives = {m['nombre']: m for m in cursor.fetchall()}
             existing_codes = {m['system_code'] for m in existing_motives.values() if m['system_code']}

             for m in stock_motives:
                 name = m['descripcion']
                 sys_code = m.get('system_code')
                 
                 found = False
                 if name in existing_motives: found = True
                 if sys_code and sys_code in existing_codes: found = True
                 
                 if not found:
                     # Map old columns to new
                     tipo_map = 'AJUSTE' 
                     # old tipo: 'alta', 'baja'
                     old_tipo = m.get('tipo', '').lower()
                     if old_tipo == 'alta': tipo_map = 'ENTRADA'
                     elif old_tipo == 'baja': tipo_map = 'SALIDA'
                     
                     es_pend = m.get('es_pendiente', 0)
                     ent_id = m.get('enterprise_id', 1)
                     
                     cursor.execute("""
                        INSERT INTO stk_motivos (enterprise_id, nombre, tipo, es_pendiente, system_code, automatico)
                        VALUES (%s, %s, %s, %s, %s, 0)
                     """, (ent_id, name, tipo_map, es_pend, sys_code))
                     print(f"Migrated motive: {name}")
        
        conn.commit()
        conn.close()
        print("Schema update completed.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_schema()
