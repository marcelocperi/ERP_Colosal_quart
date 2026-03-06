import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # Check if Repuestos exists
    cursor.execute("SELECT * FROM stk_tipos_articulo WHERE nombre LIKE '%Repuesto%'")
    rows = cursor.fetchall()
    
    if not rows:
        print("Tipo 'Repuestos' not found. Creating it...")
        cursor.execute("INSERT INTO stk_tipos_articulo (enterprise_id, nombre, descripcion) VALUES (1, 'Repuestos', 'Artículos de repuesto y componentes técnicos')")
        conn.commit()
        print(f"Created 'Repuestos' with ID: {cursor.lastrowid}")
    else:
        for row in rows:
            print(f"Found: {row}")
            
    conn.close()
except Exception as e:
    print(f"Error: {e}")
