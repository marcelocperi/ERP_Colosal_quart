import mariadb
import sys
from database import DB_CONFIG

def update_articulos_types():
    print("Iniciando actualización de tipos de artículo...")
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except mariadb.Error as e:
        print(f"Error conectando a DB: {e}")
        return

    try:
        # 1. Crear tabla stk_tipos_articulo
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_tipos_articulo (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                nombre VARCHAR(100) NOT NULL,
                descripcion VARCHAR(255),
                usa_api_libros BOOLEAN DEFAULT 0,
                activo BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Tabla 'stk_tipos_articulo' validada/creada.")

        # 2. Insertar tipo 'Libros' con ID 1 si no existe
        # Nota: Usamos INSERT IGNORE o comprobamos existencia para evitar errores si ya existe el ID.
        cursor.execute("SELECT id FROM stk_tipos_articulo WHERE id = 1")
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO stk_tipos_articulo (id, enterprise_id, nombre, descripcion, usa_api_libros)
                VALUES (1, 1, 'Libros', 'Artículos gestionados como libros (Biblioteca)', 1)
            """)
            print("Tipo 'Libros' (ID 1) insertado.")
        else:
            # Aseguramos que tenga usa_api_libros = 1
            cursor.execute("UPDATE stk_tipos_articulo SET usa_api_libros = 1 WHERE id = 1")

        # 3. Agregar columna tipo_articulo_id a stk_articulos si no existe
        cursor.execute("SHOW COLUMNS FROM stk_articulos LIKE 'tipo_articulo_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE stk_articulos ADD COLUMN tipo_articulo_id INT AFTER categoria_id")
            print("Columna 'tipo_articulo_id' agregada a stk_articulos.")

        # 4. Vincular todos los artículos existentes al tipo 'Libros' (ID 1)
        # Solo lo hacemos para los que tienen tipo_articulo_id NULL
        cursor.execute("UPDATE stk_articulos SET tipo_articulo_id = 1 WHERE tipo_articulo_id IS NULL")
        print("Artículos existentes vinculados al tipo 'Libros'.")

        conn.commit()
        print("Proceso de base de datos finalizado exitosamente.")
    except mariadb.Error as e:
        print(f"Error en base de datos: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_articulos_types()
