import os
import sys

# Agregar el directorio raíz al path de Python para poder importar desde multiMCP
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from multiMCP.database import get_db_cursor

def create_printers_table():
    print("Iniciando creación de tabla stk_impresoras_config...")
    try:
        with get_db_cursor() as cursor:
            # Create stk_impresoras_config table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_impresoras_config (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    nombre VARCHAR(100) NOT NULL,
                    marca VARCHAR(50) DEFAULT 'Zebra',
                    modelo VARCHAR(50) DEFAULT '',
                    ancho_mm DECIMAL(10,2) DEFAULT 100.00,
                    alto_mm DECIMAL(10,2) DEFAULT 50.00,
                    es_predeterminada TINYINT(1) DEFAULT 0,
                    activo TINYINT(1) DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (enterprise_id) REFERENCES sys_enterprises(id) ON DELETE CASCADE
                )
            """)
            print("Tabla 'stk_impresoras_config' creada o ya existe.")

            # Create default printers for existing enterprises
            cursor.execute("SELECT id FROM sys_enterprises")
            enterprises = cursor.fetchall()
            
            for (ent_id,) in enterprises:
                cursor.execute("SELECT id FROM stk_impresoras_config WHERE enterprise_id = %s", (ent_id,))
                if not cursor.fetchone():
                    # Default Zebra 100x50
                    cursor.execute("""
                        INSERT INTO stk_impresoras_config 
                        (enterprise_id, nombre, marca, modelo, ancho_mm, alto_mm, es_predeterminada) 
                        VALUES (%s, 'Zebra Estándar 4x2', 'Zebra', 'Genérico', 100.00, 50.00, 1)
                    """, (ent_id,))
            print("Impresoras por defecto asignadas a las empresas existentes.")

        print("Fase 1.2 Setup de Impresoras completado exitosamente.")
    except Exception as e:
        print(f"Error durante el setup de impresoras: {e}")

if __name__ == "__main__":
    create_printers_table()
