from database import get_db_cursor
import os

def apply_schema():
    schema_file = 'pricing_schema.sql'
    if not os.path.exists(schema_file):
        print(f"Error: {schema_file} not found.")
        return

    with open(schema_file, 'r', encoding='utf-8') as f:
        sql = f.read()

    # Split by semicolon
    commands = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]

    try:
        with get_db_cursor() as cursor:
            # First, drop in reverse order to avoid FK issues
            tables_to_drop = [
                'stk_articulos_precios',
                'stk_pricing_reglas',
                'stk_pricing_formulas',
                'stk_listas_precios',
                'stk_metodos_costeo'
            ]
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            for table in tables_to_drop:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            for cmd in commands:
                try:
                    cursor.execute(cmd)
                except Exception as e:
                    print(f"Error executing command: {e}")
                    print(f"Command: {cmd}")
        print("Schema applied successfully with fresh tables.")
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == '__main__':
    apply_schema()
