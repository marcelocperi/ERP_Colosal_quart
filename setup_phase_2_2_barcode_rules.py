import os
import sys

# Agregar el directorio raíz al path de Python para poder importar desde multiMCP
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from multiMCP.database import get_db_cursor

def create_barcode_rules_table():
    print("Iniciando creación de tabla stk_barcode_rules...")
    try:
        with get_db_cursor() as cursor:
            # Create stk_barcode_rules table
            # Este motor permite definir cómo interpretar códigos EAN-13 dinámicos (balanzas)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_barcode_rules (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    prefijo VARCHAR(5) NOT NULL,
                    descripcion VARCHAR(100),
                    tipo_valor ENUM('PESO', 'PRECIO', 'CANTIDAD') DEFAULT 'PESO',
                    pos_prod_inicio INT DEFAULT 2,
                    pos_prod_fin INT DEFAULT 7,
                    pos_val_inicio INT DEFAULT 7,
                    pos_val_fin INT DEFAULT 12,
                    divisor DECIMAL(10,3) DEFAULT 1000.000,
                    activo TINYINT(1) DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (enterprise_id) REFERENCES sys_enterprises(id) ON DELETE CASCADE
                )
            """)
            print("Tabla 'stk_barcode_rules' creada.")

            # Insertar regla estándar por defecto (Prefijo 21 para Peso en gramos)
            cursor.execute("SELECT id FROM sys_enterprises")
            enterprises = cursor.fetchall()
            for (ent_id,) in enterprises:
                cursor.execute("SELECT id FROM stk_barcode_rules WHERE enterprise_id = %s AND prefijo = '21'", (ent_id,))
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO stk_barcode_rules 
                        (enterprise_id, prefijo, descripcion, tipo_valor, pos_prod_inicio, pos_prod_fin, pos_val_inicio, pos_val_fin, divisor)
                        VALUES (%s, '21', 'Balanza Estándar (Peso en gramos)', 'PESO', 2, 7, 7, 12, 1000.000)
                    """, (ent_id,))
            print("Reglas estándar insertadas.")

    except Exception as e:
        print(f"Error durante el setup de reglas de barcode: {e}")

if __name__ == "__main__":
    create_barcode_rules_table()
