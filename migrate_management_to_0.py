import logging
from database import get_db_cursor

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_table(table, source_ids, target_id, cols_to_copy, unique_cols):
    """Generic migration function that copies rows from source_ids to target_id."""
    with get_db_cursor(dictionary=True) as cursor:
        cols_str = ", ".join(cols_to_copy)
        placeholders = ", ".join(["%s"] * (len(cols_to_copy) + 1))
        
        insert_query = f"INSERT IGNORE INTO `{table}` (enterprise_id, {cols_str}) VALUES ({target_id}, {placeholders[:-4]})"
        # Wait, placeholders logic above is slightly off if I manually put target_id.
        # Let's fix.
        
        placeholders = ", ".join(["%s"] * len(cols_to_copy))
        query = f"INSERT IGNORE INTO `{table}` (enterprise_id, {cols_str}) VALUES (%s, {placeholders})"
        
        for sid in source_ids:
            cursor.execute(f"SELECT {cols_str} FROM `{table}` WHERE enterprise_id = %s", (sid,))
            rows = cursor.fetchall()
            for row in rows:
                values = [target_id] + [row[c] for c in cols_to_copy]
                cursor.execute(query, values)
        logger.info(f"Finished migrating {table} to enterprise {target_id}")

def migrate_roles_and_permissions(source_ids, target_id):
    """Special migration for roles and permissions to maintain relationships."""
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Migrate Permissions
        migrate_table('sys_permissions', source_ids, target_id, ['code', 'description', 'category'], ['code'])
        
        # 2. Migrate Roles
        migrate_table('sys_roles', source_ids, target_id, ['name', 'description'], ['name'])
        
        # 3. Migrate Role-Permission Relationships
        # We need to map role names and permission codes because IDs will differ.
        for sid in source_ids:
            cursor.execute("""
                SELECT r.name as role_name, p.code as perm_code
                FROM sys_role_permissions rp
                JOIN sys_roles r ON rp.role_id = r.id
                JOIN sys_permissions p ON rp.permission_id = p.id
                WHERE rp.enterprise_id = %s
            """, (sid,))
            rels = cursor.fetchall()
            
            for rel in rels:
                # Find new IDs in enterprise 0
                cursor.execute("SELECT id FROM sys_roles WHERE enterprise_id = %s AND name = %s", (target_id, rel['role_name']))
                role_row = cursor.fetchone()
                cursor.execute("SELECT id FROM sys_permissions WHERE enterprise_id = %s AND code = %s", (target_id, rel['perm_code']))
                perm_row = cursor.fetchone()
                
                if role_row and perm_row:
                    cursor.execute("""
                        INSERT IGNORE INTO sys_role_permissions (enterprise_id, role_id, permission_id)
                        VALUES (%s, %s, %s)
                    """, (target_id, role_row['id'], perm_row['id']))

def run_migration():
    source_ids = [1, 4]
    target_id = 0
    
    # 1. Roles & Perms
    migrate_roles_and_permissions(source_ids, target_id)
    
    # 2. stock_motivos (Check if it's stk_motivos or stock_motivos, I'll do both if they exist)
    tables_to_migrate = [
        ('stock_motivos', ['descripcion', 'tipo', 'es_pendiente', 'system_code']),
        ('stk_motivos', ['codigo', 'descripcion', 'tipo', 'color', 'icon', 'es_sistema', 'orden', 'activo']),
        ('stk_tipos_articulo', ['nombre', 'descripcion', 'usa_api_libros', 'activo']),
        ('sys_tipos_comprobante', ['codigo', 'descripcion', 'letra', 'proximo_numero', 'puesto_id', 'activo']),
    ]
    
    for table, cols in tables_to_migrate:
        try:
            migrate_table(table, source_ids, target_id, cols, [])
        except Exception as e:
            logger.warning(f"Skipping {table}: {e}")

if __name__ == "__main__":
    run_migration()
