import sys
import os
from datetime import datetime
import io

# Force UTF-8 for output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Añadir el directorio actual al path para importar database
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

try:
    from database import get_db_cursor
except ImportError:
    print("❌ Error: No se pudo encontrar el módulo 'database'.")
    sys.exit(1)

def check_latest_incident(limit=1):
    """
    Lee los últimos incidentes de error de la tabla sys_transaction_logs.
    """
    print(f"\n--- REVISANDO ÚLTIMOS {limit} INCIDENTES ({datetime.now().strftime('%H:%M:%S')}) ---")
    
    query = """
        SELECT 
            id, 
            enterprise_id, 
            user_id, 
            module, 
            endpoint, 
            request_method, 
            error_message, 
            error_traceback, 
            created_at 
        FROM sys_transaction_logs 
        WHERE status = 'ERROR' 
        ORDER BY created_at DESC 
        LIMIT %s
    """
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute(query, (limit,))
            incidents = cursor.fetchall()
            
            if not incidents:
                print("✅ No se encontraron incidentes de error recientes.")
                return

            for inc in incidents:
                print(f"\n🆔 INCIDENTE #{inc['id']} | 📅 {inc['created_at']}")
                print(f"📍 Módulo: {inc['module']} | Método: {inc['request_method']}")
                print(f"🔗 Endpoint: {inc['endpoint']}")
                print(f"🏢 Enterprise ID: {inc['enterprise_id']} | 👤 User ID: {inc['user_id']}")
                print("-" * 50)
                print(f"❌ ERROR: {inc['error_message']}")
                print("-" * 50)
                if inc['error_traceback']:
                    print("🔍 TRACEBACK:")
                    print(inc['error_traceback'])
                else:
                    print("🔍 Sin traceback detallado.")
                print("=" * 60)

    except Exception as e:
        print(f"❌ Error al consultar la base de datos: {str(e)}")

if __name__ == "__main__":
    # Parse args
    limit = 1
    if len(sys.argv) > 1:
        for i, arg in enumerate(sys.argv):
            if arg == '-n' and i + 1 < len(sys.argv) and sys.argv[i+1].isdigit():
                limit = int(sys.argv[i+1])
                break
            elif arg.isdigit():
                limit = int(arg)
                break
        
    check_latest_incident(limit)
