import os
import sys

# Agregar el directorio raíz al path de Python para poder importar desde multiMCP
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from multiMCP.database import get_db_cursor

def populate_scales():
    print("Populando balanzas conocidas en stk_balanzas_config...")
    try:
        with get_db_cursor() as cursor:
            # Obtener todas las empresas
            cursor.execute("SELECT id FROM sys_enterprises")
            enterprises = cursor.fetchall()
            
            for (ent_id,) in enterprises:
                # Verificar si ya tiene balanzas cargadas para no duplicar
                cursor.execute("SELECT id FROM stk_balanzas_config WHERE enterprise_id = %s", (ent_id,))
                if cursor.fetchone():
                    print(f"La empresa {ent_id} ya tiene balanzas configuradas. Saltando...")
                    continue

                scales = [
                    # Ethernet/WiFi - Systel Cuora (Líder en retail)
                    {
                        'nombre': 'Systel Cuora Central',
                        'marca': 'Systel',
                        'modelo': 'Cuora Max',
                        'nro_serie': 'SN-CUORA-001',
                        'tipo': 'IP_RED',
                        'ip': '192.168.1.150',
                        'predet': 1
                    },
                    # Serial/USB - Systel Croma (Mostrador tradicional)
                    {
                        'nombre': 'Systel Croma Mostrador',
                        'marca': 'Systel',
                        'modelo': 'Croma 31kg',
                        'nro_serie': 'SN-CROMA-992',
                        'tipo': 'SERIAL_USB',
                        'ip': '',
                        'predet': 0
                    },
                    # Ticket Parsing - Kretz Report (Autoservicios)
                    {
                        'nombre': 'Kretz Report - Autoservicio',
                        'marca': 'Kretz',
                        'modelo': 'Report NX',
                        'nro_serie': 'SN-KRETZ-NX-05',
                        'tipo': 'BROWSER_TICKET',
                        'ip': '',
                        'predet': 0
                    },
                    # Software Sync - Dibal (Pesaje Industrial/Seccionado)
                    {
                        'nombre': 'Dibal D-900 (Fiambrería)',
                        'marca': 'Dibal',
                        'modelo': 'D-900',
                        'nro_serie': 'SN-DIBAL-550',
                        'tipo': 'SOFTWARE_SYNC',
                        'ip': '192.168.1.160',
                        'predet': 0
                    },
                    # RS232 - Honeywell/Epson (Scanner-Balanza Integrada Check-out)
                    {
                        'nombre': 'Honeywell Stratos (Check-out 1)',
                        'marca': 'Honeywell',
                        'modelo': '2400 Bi-optic',
                        'nro_serie': 'SN-HONEY-B01',
                        'tipo': 'SERIAL_USB',
                        'ip': '',
                        'predet': 0
                    }
                ]

                for s in scales:
                    cursor.execute("""
                        INSERT INTO stk_balanzas_config 
                        (enterprise_id, nombre, marca, modelo, numero_serie, tipo_conexion, ip_red, es_predeterminada)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (ent_id, s['nombre'], s['marca'], s['modelo'], s['nro_serie'], s['tipo'], s['ip'], s['predet']))
            
            print(f"Balanzas populadas exitosamente para {len(enterprises)} empresas.")

    except Exception as e:
        print(f"Error al popular balanzas: {e}")

if __name__ == "__main__":
    populate_scales()
