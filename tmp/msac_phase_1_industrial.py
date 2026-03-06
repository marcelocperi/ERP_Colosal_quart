
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def execute_phase_1_1_v3():
    """
    Actualización de Fase 1.1: Maestro de Orígenes Industriales.
    Agrega los tipos de origen: Producción Propia y Fazón (Terceros).
    """
    try:
        with get_db_cursor() as cursor:
            print("--- Actualizando Maestro de Orígenes (Fase 1.1 Industrial) ---")
            
            # 1. Insertar orígenes industriales si no existen
            origenes = [
                ('Producción Propia', 'Insumos + Gastos Indirectos + Mano de Obra interna.'),
                ('Fazón (Terceros)', 'Servicio de fabricación externo con partes consignadas.'),
            ]
            
            for nombre, desc in origenes:
                cursor.execute("SELECT id FROM cmp_sourcing_origenes WHERE nombre = %s AND (enterprise_id = 0 OR enterprise_id = 1)", (nombre,))
                if not cursor.fetchone():
                    print(f"  > Creando origen: {nombre}")
                    cursor.execute("""
                        INSERT INTO cmp_sourcing_origenes (enterprise_id, nombre, descripcion, user_id)
                        VALUES (1, %s, %s, 1)
                    """, (nombre, desc))
            
            # 2. Crear tabla de BOM (Bill of Materials) para Fase 1.2
            print("--- Creando Estructura de BOM (Fase 1.2 Industrial) ---")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cmp_recetas_bom (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    producto_id INT NOT NULL, -- El articulo "Padre"
                    nombre_variante VARCHAR(100),
                    version VARCHAR(10) DEFAULT '1.0',
                    activo BOOLEAN DEFAULT 1,
                    instrucciones TEXT,
                    user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX(producto_id),
                    INDEX(enterprise_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cmp_recetas_detalle (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    receta_id INT NOT NULL,
                    articulo_id INT NOT NULL, -- El "Insumo"
                    cantidad DECIMAL(18,4) NOT NULL,
                    unidad_medida VARCHAR(20),
                    porcentaje_merma_esperada DECIMAL(5,2) DEFAULT 0.00,
                    es_consignado BOOLEAN DEFAULT 0, -- Para Fazón
                    user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (receta_id) REFERENCES cmp_recetas_bom(id) ON DELETE CASCADE,
                    INDEX(articulo_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 3. Tabla para Gastos y Margenes (Fase 1.3 Anticipada)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cmp_articulos_costos_indirectos (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    articulo_id INT NOT NULL,
                    tipo_gasto ENUM('MANO_OBRA', 'ENERGIA', 'AMORTIZACION', 'LOGISTICA', 'OTROS') NOT NULL,
                    monto_estimado DECIMAL(18,4) NOT NULL,
                    porcentaje_margen_esperado DECIMAL(5,2) DEFAULT 0.00,
                    user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX(articulo_id),
                    INDEX(enterprise_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            print("ESTRUCTURA INDUSTRIAL CARGADA EXITOSAMENTE.")

    except Exception as e:
        print(f"Error en ejecución industrial: {e}")

if __name__ == "__main__":
    execute_phase_1_1_v3()
