import os
import sys

# Agregar el directorio raíz al path de Python para poder importar desde multiMCP
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from multiMCP.database import get_db_cursor

def create_scales_table():
    print("Iniciando creación de tabla stk_balanzas_config...")
    try:
        with get_db_cursor() as cursor:
            # Create stk_balanzas_config table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_balanzas_config (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    nombre VARCHAR(100) NOT NULL,
                    marca VARCHAR(50) DEFAULT 'Systel',
                    modelo VARCHAR(50) DEFAULT '',
                    numero_serie VARCHAR(50) DEFAULT '',
                    tipo_conexion ENUM('IP_RED', 'SERIAL_USB', 'BROWSER_TICKET', 'SOFTWARE_SYNC') DEFAULT 'IP_RED',
                    ip_red VARCHAR(50) DEFAULT '',
                    puerto_red INT DEFAULT 9100,
                    es_predeterminada TINYINT(1) DEFAULT 0,
                    activo TINYINT(1) DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (enterprise_id) REFERENCES sys_enterprises(id) ON DELETE CASCADE
                )
            """)
            print("Tabla 'stk_balanzas_config' creada o ya existe.")

        print("Fase 2.1 Setup de Balanzas completado exitosamente.")
    except Exception as e:
        print(f"Error durante el setup de balanzas: {e}")

if __name__ == "__main__":
    create_scales_table()
