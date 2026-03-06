
from database import get_db_cursor
from services.georef_service import GeorefService
import logging

logging.basicConfig(level=logging.INFO)

def seed_defaults():
    print("--- Seeding Puestos ---")
    try:
        with get_db_cursor() as cursor:
            # Obtener todas las empresas activas
            cursor.execute("SELECT id FROM sys_enterprises WHERE estado = 'activo'")
            enterprises = cursor.fetchall()
            
            defaults = [
                ('Administración', 'Gerente'), ('Administración', 'Administrativo'), ('Administración', 'Contable'),
                ('Ventas', 'Ejecutivo de Ventas'), ('Ventas', 'Vendedor'), ('Ventas', 'Cobranzas'),
                ('Compras', 'Comprador'), ('Compras', 'Responsable de Compras'),
                ('Logística', 'Depósito'), ('Logística', 'Operador')
            ]
            
            for ent in enterprises:
                ent_id = ent[0]
                # Verificar si ya tiene puestos
                cursor.execute("SELECT COUNT(*) FROM erp_puestos WHERE enterprise_id = %s", (ent_id,))
                count = cursor.fetchone()[0]
                if count == 0:
                    print(f"Sembrando puestos para empresa {ent_id}...")
                    for area, nombre in defaults:
                        cursor.execute("INSERT INTO erp_puestos (enterprise_id, area, nombre) VALUES (%s, %s, %s)", (ent_id, area, nombre))
                else:
                    print(f"Empresa {ent_id} ya tiene {count} puestos.")
    except Exception as e:
        print(f"Error sembrando puestos: {e}")

    print("\n--- Seeding Provincias ---")
    try:
        provincias = GeorefService.get_provincias()
        if not provincias:
            print("Cargando provincias desde API Georef...")
            GeorefService.load_provincias()
        else:
            print(f"Ya existen {len(provincias)} provincias en la base de datos.")
    except Exception as e:
        print(f"Error sembrando provincias: {e}")

if __name__ == "__main__":
    seed_defaults()
