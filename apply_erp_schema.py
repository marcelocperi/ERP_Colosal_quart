import mariadb
import os
from database import DB_CONFIG

def apply_schema():
    print("Conectando a la base de datos...")
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except mariadb.Error as e:
        print(f"Error conectando a DB: {e}")
        return

    schema_file = 'erp_schema.sql'
    if not os.path.exists(schema_file):
        print(f"No se encuentra {schema_file}")
        return

    print(f"Leyendo {schema_file}...")
    with open(schema_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Split by semicolon, naive approach but works for this schema structure
    # Removing empty statements
    statements = [s.strip() for s in sql_content.split(';') if s.strip()]

    print(f"Encontradas {len(statements)} instrucciones SQL.")

    for i, statement in enumerate(statements):
        try:
            # Skip comments-only blocks if any remain
            if statement.startswith('--'):
                 lines = statement.split('\n')
                 statement = '\n'.join([l for l in lines if not l.strip().startswith('--')])
            
            if not statement.strip(): continue

            cursor.execute(statement)
            # print(f"Ejecutado {i+1}/{len(statements)}")
        except mariadb.Error as e:
            # Ignore "Table already exists" errors to allow idempotency
            error_msg = str(e)
            if "already exists" in error_msg:
                print(f"Nota: Tabla ya existe. ({error_msg})")
            elif "Duplicate entry" in error_msg:
                print(f"Nota: Dato duplicado (probablemente seed data ya insertada).")
            else:
                print(f"ERROR ejecutando sentencia {i+1}: {e}\nSQL: {statement[:100]}...")
                # conn.rollback() # Optional: rollback on error? For schema migration sometimes we want to proceed
    
    conn.commit()
    conn.close()
    print("Migración de esquema ERP finalizada exitosamente.")

if __name__ == "__main__":
    apply_schema()
