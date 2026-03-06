from database import get_db_cursor
try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT l.*, u.username as user_name, p.nombre as provincia_nombre
            FROM log_erp_terceros_cm05 l
            LEFT JOIN sys_users u ON l.user_action = u.id
            LEFT JOIN sys_provincias p ON (l.jurisdiccion_code COLLATE utf8mb4_uca1400_ai_ci) = LPAD(p.id, 3, '0')
            WHERE l.tercero_id = 469
            ORDER BY l.fecha_efectiva DESC
        """)
        print("Query successful, results:", len(cursor.fetchall()))
except Exception as e:
    print("Query failed:", str(e))
