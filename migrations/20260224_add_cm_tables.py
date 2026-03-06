import sys
import os

# Agregamos el path raíz para poder importar `database`
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor

def forward():
    print("Migrando tablas para Convenio Multilateral (CM01 y CM05)...")
    
    with get_db_cursor() as cursor:
        try:
            # 1. Tabla para CM01: Alta de Jurisdicciones por Tercero
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_terceros_jurisdicciones (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                tercero_id INT NOT NULL,
                jurisdiccion_code VARCHAR(10) NOT NULL COMMENT 'Ej: 901 para CABA, 902 para PBA',
                numero_inscripcion VARCHAR(50) NULL COMMENT 'Nro de Ingresos Brutos en esa jurisdicción',
                fecha_alta DATE NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE,
                FOREIGN KEY (enterprise_id) REFERENCES sys_enterprises(id) ON DELETE CASCADE,
                UNIQUE KEY uk_tercero_jur (enterprise_id, tercero_id, jurisdiccion_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            print("Tabla erp_terceros_jurisdicciones (CM01) creada/verificada correctamente.")

            # 2. Tabla para CM05: Coeficientes Unificados Anuales por Tercero
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_terceros_cm05 (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                tercero_id INT NOT NULL,
                jurisdiccion_code VARCHAR(10) NOT NULL,
                periodo_anio INT NOT NULL COMMENT 'Año al que corresponde el coeficiente (ej. 2024)',
                coeficiente DECIMAL(10,4) NOT NULL COMMENT 'Coeficiente unificado, ej. 0.5000 para el 50%',
                fecha_presentacion DATE NULL COMMENT 'Cuándo se presentó la DDJJ CM05',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE,
                FOREIGN KEY (enterprise_id) REFERENCES sys_enterprises(id) ON DELETE CASCADE,
                UNIQUE KEY uk_tercero_cm05 (enterprise_id, tercero_id, jurisdiccion_code, periodo_anio)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            print("Tabla erp_terceros_cm05 (CM05) creada/verificada correctamente.")

            # Opcional: Agregar campo a erp_terceros si es agente de ret/per general, o si tiene convenio.
            # Podríamos agregar p.ej: 'es_convenio_multilateral' BOOLEAN DEFAULT FALSE
            # Revisemos si existen antes de añadir
            cursor.execute("SHOW COLUMNS FROM erp_terceros LIKE 'es_convenio_multilateral'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE erp_terceros ADD COLUMN es_convenio_multilateral BOOLEAN DEFAULT FALSE COMMENT 'Indica si tributa por CM01/05';")
                print("Agregado campo 'es_convenio_multilateral' a erp_terceros.")

            print("Migración de CM01/CM05 completada exitosamente.")

        except Exception as e:
            print(f"Error en la migración: {e}")
            raise

if __name__ == "__main__":
    forward()
