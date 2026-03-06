import sys
import os

# Asegurar que el directorio actual esté en el path para importar database.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from database import get_db_cursor
except ImportError as e:
    print(f"Error al importar database: {e}")
    sys.exit(1)

def setup_industrial_safety():
    print("--- Iniciando Fase 5.1: Estructura de Seguridad Industrial (GHS/SGA) ---")
    
    try:
        with get_db_cursor() as cursor:
            # 1. Tabla Maestra de Pictogramas (Global)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sys_ghs_pictogramas (
                    codigo VARCHAR(10) PRIMARY KEY,
                    nombre VARCHAR(100),
                    descripcion TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            # Seed Pictogramas
            pictogramas = [
                ('GHS01', 'Explosivos', 'Bomba explotando'),
                ('GHS02', 'Inflamables', 'Llama'),
                ('GHS03', 'Comburentes', 'Llama sobre un círculo (Oxidantes)'),
                ('GHS04', 'Gases a presión', 'Bombona de gas'),
                ('GHS05', 'Corrosivos', 'Corrosión de metales y piel'),
                ('GHS06', 'Toxicidad aguda', 'Calavera y tibias cruzadas (Veneno)'),
                ('GHS07', 'Peligro menor', 'Signo de exclamación (Irritante/Narcótico)'),
                ('GHS08', 'Peligro grave salud', 'Mutagénico, carcinogénico, toxicidad respiratoria'),
                ('GHS09', 'Peligro medio ambiente', 'Toxicidad acuática (Pez y árbol)')
            ]
            
            cursor.executemany("""
                INSERT IGNORE INTO sys_ghs_pictogramas (codigo, nombre, descripcion)
                VALUES (%s, %s, %s)
            """, pictogramas)
            print(f"✅ Cargados {len(pictogramas)} pictogramas estándar GHS.")

            # 2. Información de Seguridad por Artículo
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_articulos_seguridad (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    articulo_id INT NOT NULL,
                    enterprise_id INT NOT NULL,
                    numero_un VARCHAR(10) COMMENT 'Nº ONU',
                    clase_riesgo VARCHAR(50),
                    nombre_tecnico VARCHAR(255),
                    instrucciones_estibaje TEXT,
                    frases_h TEXT COMMENT 'Frases de Peligro',
                    frases_p TEXT COMMENT 'Frases de Prudencia',
                    pictogramas_json JSON COMMENT 'Array de códigos GHS (ej: ["GHS02", "GHS06"])',
                    forma_estibaje VARCHAR(100) COMMENT 'Ej: Apilado máx 3, Temperatura < 25C',
                    incompatibilidades TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE(articulo_id, enterprise_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            print("✅ Tabla stk_articulos_seguridad creada.")

            # 3. Permisos para el nuevo módulo
            # Ajustado a la estructura real: (code, description, category, enterprise_id)
            cursor.execute("""
                INSERT IGNORE INTO sys_permissions (code, description, category, enterprise_id) 
                VALUES (%s, %s, %s, %s)
            """, ('industrial_safety', 'Gestionar normativas de seguridad y etiquetas SGA/GHS', 'STOCK', 0))
            print("✅ Permiso 'industrial_safety' registrado.")

        print("--- Fase 5.1 Completada con éxito ---")
    except Exception as ex:
        print(f"❌ Error durante el setup: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    setup_industrial_safety()
