
import sys
import os
sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor
from datetime import datetime

def enroll_msac_suggestions():
    """
    Inscribe las funcionalidades de MSAC Industrial y Consignación 
    en la tabla de sugerencias pendientes (sys_roadmap_decisions).
    """
    suggestions = [
        ('COMPRAS', 'INDUSTRIAL', 'BOM Multi-Origen', 
         'Lógica de explosión de materiales (BOM) vinculada a orígenes locales e importados en cotizaciones.', 
         'Pendiente'),
        
        ('COMPRAS', 'CONSIGNACION', 'Consignación 360° (Fazón/Tenencia)', 
         'Gestión de stock en poder de terceros con trazabilidad de consumo y devolución de remanentes.', 
         'Pendiente'),
        
        ('COMPRAS', 'CONSIGNACION', 'Calendarización de Avisos de Venta', 
         'Registro periódico de ventas del consignatario para facturación automática y control de tenencia.', 
         'Pendiente'),
        
        ('COSTOS', 'INTELLIGENCE', 'Margen Real vs Teórico Industrial', 
         'Reporte de desvíos entre el costo BOM proyectado y el consumo industrial real (Capas FIFO).', 
         'Pendiente'),

        ('FINANZAS', 'AUTOMATIZACION', 'Facturación por Liquidación de Consignación', 
         'Generación automática de facturas al confirmar el aviso de venta de un tercero sin intervención manual.', 
         'Pendiente'),
    ]
    
    try:
        with get_db_cursor() as cursor:
            print("--- Inscribiendo Sugerencias MSAC en sys_roadmap_decisions ---")
            for mod, sub, func, desc, dec in suggestions:
                # Evitar duplicados
                cursor.execute("SELECT id FROM sys_roadmap_decisions WHERE funcionalidad = %s", (func,))
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO sys_roadmap_decisions 
                        (enterprise_id, modulo, subcategoria, funcionalidad, descripcion_ampliada, decision, dt_user_created)
                        VALUES (1, %s, %s, %s, %s, %s, 1)
                    """, (mod, sub, func, desc, dec))
                    print(f"  [+] Sugerencia INSCRIPTA: {func}")
                else:
                    print(f"  [.] Ya existía: {func}")
            
            print("\nPROCESO DE INSCRIPCIÓN COMPLETADO.")

    except Exception as e:
        print(f"Error inscribiendo sugerencias: {e}")

if __name__ == "__main__":
    enroll_msac_suggestions()
