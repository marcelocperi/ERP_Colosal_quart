from database import get_db_cursor

SERVICES = [
    {
        'nombre': 'COT ARBA (Logística)',
        'tipo_servicio': 'AFIP_ARBA_PROVIDER',
        'clase_implementacion': 'services.cot_service.CotArbaService',
        'config_json': '{\n  "user": "",\n  "password": "",\n  "cuit": "",\n  "url": "https://cot.arba.gov.ar/trasladoBienes/ConsultasAction.do",\n  "environment": "testing"\n}',
        'system_code': 'COT_ARBA'
    },
    {
        'nombre': 'Padrón ARBA (IIBB)',
        'tipo_servicio': 'TAX_PADRON',
        'clase_implementacion': 'services.tax_service.ArbaPadronService',
        'config_json': '{\n  "user": "",\n  "password": "",\n  "cuit": ""\n}',
        'system_code': 'PADRON_ARBA'
    },
    {
        'nombre': 'Google Books API',
        'tipo_servicio': 'DATA_PROVIDER',
        'clase_implementacion': 'services.enrichment.GoogleBooksProvider',
        'config_json': '{\n  "api_key": ""\n}',
        'system_code': 'GOOGLE_BOOKS'
    }
]

def seed_global_services():
    with get_db_cursor() as cursor:
        print("Seeding global services (Enterprise 0)...")
        for s in SERVICES:
            # Check if exists
            cursor.execute("SELECT id FROM sys_external_services WHERE system_code = %s AND enterprise_id = 0", (s['system_code'],))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO sys_external_services (enterprise_id, nombre, tipo_servicio, clase_implementacion, config_json, system_code, activo)
                    VALUES (0, %s, %s, %s, %s, %s, 1)
                """, (s['nombre'], s['tipo_servicio'], s['clase_implementacion'], s['config_json'], s['system_code']))
                print(f" - Added {s['nombre']}")
            else:
                print(f" - {s['nombre']} already exists")

if __name__ == "__main__":
    seed_global_services()
