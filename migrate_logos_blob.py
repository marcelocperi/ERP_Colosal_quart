from database import get_db_cursor
import os

def migrate_logos_to_db():
    print("Iniciando migración de logos a base de datos (BLOB)...")
    with get_db_cursor() as cursor:
        # Get all enterprises with a logo_path
        cursor.execute("SELECT id, logo_path FROM sys_enterprises WHERE logo_path IS NOT NULL AND logo_path != ''")
        enterprises = cursor.fetchall()
        
        for ent_id, logo_path in enterprises:
            # Skip if already migrated (starts with /sysadmin/)
            if logo_path.startswith('/sysadmin/'):
                print(f"Empresa {ent_id}: Logo ya migrado ({logo_path})")
                continue
            
            # Clean path (remove leading /)
            rel_path = logo_path.lstrip('/')
            abs_path = os.path.join(os.getcwd(), rel_path)
            
            if os.path.exists(abs_path):
                print(f"Empresa {ent_id}: Migrando {logo_path}...")
                try:
                    with open(abs_path, 'rb') as f:
                        data = f.read()
                    
                    ext = logo_path.rsplit('.', 1)[1].lower() if '.' in logo_path else 'png'
                    mime = f"image/{ext}" if ext != 'jpg' else 'image/jpeg'
                    
                    # Insert into BLOB table
                    cursor.execute("""
                        INSERT INTO sys_enterprise_logos (enterprise_id, logo_data, mime_type, is_active)
                        VALUES (?, ?, ?, 1)
                    """, (ent_id, data, mime))
                    logo_id = cursor.lastrowid
                    
                    # Update logo_path
                    new_path = f"/sysadmin/enterprises/logo/raw/{logo_id}"
                    cursor.execute("UPDATE sys_enterprises SET logo_path = ? WHERE id = ?", (new_path, ent_id))
                    print(f"Empresa {ent_id}: OK -> {new_path}")
                except Exception as e:
                    print(f"Error migrando empresa {ent_id}: {e}")
            else:
                print(f"Empresa {ent_id}: Archivo no encontrado en {abs_path}")

if __name__ == "__main__":
    migrate_logos_to_db()
