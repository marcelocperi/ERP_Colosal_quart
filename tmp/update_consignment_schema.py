
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def update_consignment_tables():
    """
    Agrega campos de Calendarización y Facturación para el flujo de avisos de venta.
    """
    try:
        with get_db_cursor() as cursor:
            # 1. Facturas pro-forma o avisos de venta
            cursor.execute("""
                ALTER TABLE cmp_liquidaciones_consignacion 
                ADD COLUMN IF NOT EXISTS estado_facturacion ENUM('PENDIENTE', 'FACTURADO', 'CANCELADO') DEFAULT 'PENDIENTE',
                ADD COLUMN IF NOT EXISTS fecha_programada_factura DATETIME NULL,
                ADD COLUMN IF NOT EXISTS observaciones_liquidacion TEXT NULL,
                ADD COLUMN IF NOT EXISTS nro_aviso_venta VARCHAR(50) NULL;
            """)
            
            # 2. Fecha de liquidacion automatica o reportar venta (Calendarizacion)
            cursor.execute("""
                ALTER TABLE cmp_consignaciones
                ADD COLUMN IF NOT EXISTS frecuencia_liquidacion_dias INT DEFAULT 30, -- Cada cuanto se espera aviso de venta
                ADD COLUMN IF NOT EXISTS proxima_fecha_revision DATETIME NULL;
            """)
            
            print("CAMPOS DE FACTURACION Y CALENDARIZACION AGREGADOS CORRECTAMENTE.")

    except Exception as e:
        print(f"Error en actualización de tablas: {e}")

if __name__ == "__main__":
    update_consignment_tables()
