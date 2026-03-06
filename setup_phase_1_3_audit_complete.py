import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def upgrade_audit_for_all_facts():
    print("Expandiendo Motor de Trazabilidad para Hechos de Inventario (Traslados, Ajustes)...")
    with get_db_cursor() as cursor:
        # 1. Expandir ENUM de tipo_evento
        try:
            cursor.execute("""
                ALTER TABLE stk_series_trazabilidad 
                MODIFY COLUMN tipo_evento ENUM('INGRESO','VENTA','DEVOLUCION','AJUSTE','BAJA','TRASLADO') NOT NULL
            """)
            print("  ENUM tipo_evento expandido (incluye TRASLADO).")
        except Exception as e:
            print(f"  Error expandiendo ENUM: {e}")

        # 2. Agregar deposito_id para saber dónde está el serial en cada tramo
        try:
            cursor.execute("ALTER TABLE stk_series_trazabilidad ADD COLUMN deposito_id INT DEFAULT NULL AFTER tercero_id")
            print("  Columna deposito_id agregada.")
        except:
            pass

        # 3. Agregar referencia_externa para documentos que no están en erp_comprobantes
        try:
            cursor.execute("ALTER TABLE stk_series_trazabilidad ADD COLUMN referencia_identificador VARCHAR(100) DEFAULT NULL AFTER comprobante_id")
            print("  Columna referencia_identificador agregada.")
        except:
            pass

    print("Motor de Auditoría listo para Hechos de Inventario de Ciclo Completo.")

if __name__ == "__main__":
    upgrade_audit_for_all_facts()
