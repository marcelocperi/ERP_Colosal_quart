import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Asegurar que las llaves existan
    stats = [
        ('batch_processed', 0, ''),
        ('batch_status', 0, 'Inactivo')
    ]
    
    for key, v_int, v_str in stats:
        cursor.execute("SELECT id FROM system_stats WHERE key_name = %s", (key,))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO system_stats (key_name, value_int, value_str) VALUES (%s, %s, %s)", (key, v_int, v_str))
        else:
            cursor.execute("UPDATE system_stats SET value_str = %s WHERE key_name = %s", (v_str, key))
            
    conn.commit()
    conn.close()
    print("✓ Tabla system_stats inicializada correctamente")
except Exception as e:
    print(f"❌ Error: {e}")
