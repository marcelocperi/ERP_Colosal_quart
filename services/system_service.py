import shutil
import mariadb
from database import DB_CONFIG

def get_db_health():
    """
    Obtiene métricas de salud de la base de datos y espacio en disco.
    """
    health = {
        "db_size_mb": 0,
        "disk_total_gb": 0,
        "disk_used_gb": 0,
        "disk_free_gb": 0,
        "disk_percent": 0,
        "status_color": "success"
    }

    try:
        # 1. Tamaño de la Base de Datos vía SQL
        db_name = DB_CONFIG.get('database', 'multi_mcp_db')
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            SELECT 
                ROUND(SUM(data_length + index_length) / 1024 / 1024, 2)
            FROM information_schema.TABLES
            WHERE table_schema = ?
        """
        cursor.execute(query, (db_name,))
        res = cursor.fetchone()
        if res:
            health["db_size_mb"] = res[0] or 0
        conn.close()

        # 2. Espacio en Disco (Directorio raíz de la base de datos)
        total, used, free = shutil.disk_usage("/")
        health["disk_total_gb"] = total // (2**30)
        health["disk_used_gb"] = used // (2**30)
        health["disk_free_gb"] = free // (2**30)
        health["disk_percent"] = round((used / total) * 100, 1)

        # 3. Lógica de alertas
        if health["disk_percent"] > 90:
            health["status_color"] = "danger"
        elif health["disk_percent"] > 75:
            health["status_color"] = "warning"

    except Exception as e:
        print(f"Error en System Service: {e}")
        health["status_color"] = "danger"

    return health
