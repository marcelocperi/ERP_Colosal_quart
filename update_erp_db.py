import mariadb
import sys
from database import DB_CONFIG

def update_db():
    print("Conectando a la base de datos para actualización...")
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except mariadb.Error as e:
        print(f"Error conectando a DB: {e}")
        return

    try:
        # 1. Modificar erp_terceros para quitar direccion y agregar observaciones
        # Verificamos si la columna direccion existe antes de intentar borrarla
        cursor.execute("SHOW COLUMNS FROM erp_terceros LIKE 'direccion'")
        if cursor.fetchone():
            cursor.execute("ALTER TABLE erp_terceros DROP COLUMN direccion")
            print("Columna 'direccion' eliminada de erp_terceros.")

        cursor.execute("SHOW COLUMNS FROM erp_terceros LIKE 'observaciones'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE erp_terceros ADD COLUMN observaciones TEXT AFTER tipo_responsable")
            print("Columna 'observaciones' agregada a erp_terceros.")

        # 2. Crear nuevas tablas si no existen
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_direcciones (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tercero_id INT NOT NULL,
                etiqueta VARCHAR(100), -- Ej: Casa Central, Depósito Pilar
                calle VARCHAR(100),
                numero VARCHAR(20),
                piso VARCHAR(10),
                depto VARCHAR(10),
                localidad VARCHAR(100),
                provincia VARCHAR(100),
                pais VARCHAR(50) DEFAULT 'Argentina',
                cod_postal VARCHAR(20),
                es_fiscal BOOLEAN DEFAULT 0,
                es_entrega BOOLEAN DEFAULT 0,
                FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE
            )
        """)
        print("Tabla erp_direcciones validada/creada.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_contactos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tercero_id INT NOT NULL,
                nombre VARCHAR(100), -- Persona de contacto
                puesto VARCHAR(100),
                tipo_contacto VARCHAR(50), -- Compras, Ventas, Tesorería, Gerencia
                telefono VARCHAR(50),
                email VARCHAR(100),
                FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE
            )
        """)
        print("Tabla erp_contactos validada/creada.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_datos_fiscales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tercero_id INT NOT NULL,
                impuesto VARCHAR(50), -- IIBB, GANANCIAS, TASAS MUNICIPALES
                jurisdiccion VARCHAR(100), -- CABA, BUENOS AIRES, CORDOBA (o "NACIONAL")
                condicion VARCHAR(50), -- Inscripto, Exento, CM (Convenio Multilateral)
                numero_inscripcion VARCHAR(50),
                fecha_vencimiento DATE, -- Para exenciones
                alicuota DECIMAL(5,2), -- % de percepción/retención específica
                FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE
            )
        """)
        print("Tabla erp_datos_fiscales validada/creada.")

        conn.commit()
        print("Actualización de base de datos finalizada exitosamente.")
    except mariadb.Error as e:
        print(f"Error durante la actualización: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_db()
