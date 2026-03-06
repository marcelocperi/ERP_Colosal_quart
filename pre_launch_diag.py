
import os
import socket

def check_port(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) != 0

def diagnose():
    print("=== DIAGNÓSTICO DE PRE-LANZAMIENTO (SERVICIO) ===")
    
    # 1. Verificar Puerto
    port = 5000
    if check_port(port):
        print(f"✅ Puerto {port} está disponible.")
    else:
        print(f"❌ Puerto {port} ya está ocupado. El servicio fallará si no cambias el puerto.")

    # 2. Verificar Variables de Entorno
    from database import DB_CONFIG
    print(f"✅ Configuración de DB detectada: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
    
    # 3. Verificar Escritura de Logs
    log_file = "service_test.log"
    try:
        with open(log_file, "w") as f:
            f.write("test")
        os.remove(log_file)
        print("✅ Permisos de escritura en el directorio actual: OK")
    except Exception as e:
        print(f"❌ Error de permisos en el directorio: {e}")

    # 4. Verificar Waitress
    try:
        import waitress
        print(f"✅ Waitress instalado (v{waitress.__version__})")
    except ImportError:
        print("❌ Waitress NO está instalado.")

if __name__ == "__main__":
    diagnose()
