from database import get_db_cursor

def create_enterprise_0():
    with get_db_cursor() as cursor:
        # Enable NO_AUTO_VALUE_ON_ZERO to allow inserting ID 0
        cursor.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'")
        
        # Check if 0 already exists
        cursor.execute("SELECT id FROM sys_enterprises WHERE id = 0")
        if cursor.fetchone():
            print("Enterprise 0 already exists.")
        else:
            try:
                cursor.execute("""
                    INSERT INTO sys_enterprises (id, codigo, nombre, estado, is_saas_owner)
                    VALUES (0, 'MASTER', 'Gestión Maestro Genérico', 'activo', 0)
                """)
                print("Enterprise 0 created successfully.")
            except Exception as e:
                print(f"Error creating enterprise 0: {e}")

if __name__ == "__main__":
    create_enterprise_0()
