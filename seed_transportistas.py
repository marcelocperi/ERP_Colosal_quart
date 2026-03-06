from database import get_db_cursor

TRANSPORTISTAS = [
    {"nombre": "Andreani Grupo Logístico", "cuit": "30528430387"},
    {"nombre": "DHL Supply Chain (Argentina) S.A.", "cuit": "30593435810"},
    {"nombre": "OCASA", "cuit": "30562723333"},
    {"nombre": "Cruz del Sur (Transportes)", "cuit": "30517865267"},
    {"nombre": "TASA Logística", "cuit": "30593231158"},
    {"nombre": "Murchison S.A.", "cuit": "30501655076"},
    {"nombre": "Federal Express Corporation (FedEx)", "cuit": "30546628902"},
    {"nombre": "Schenker Argentina S.A.", "cuit": "30708096063"},
    {"nombre": "Eagle Global Logistics de Argentina S.R.L.", "cuit": "30709160571"},
    {"nombre": "Hellmann Worldwide Logistics S.A.", "cuit": "30690212265"},
    {"nombre": "TransFarmaco S.A.", "cuit": "30590624027"},
    {"nombre": "Calico S.A.", "cuit": "30652317112"},
    {"nombre": "OCA Logística", "cuit": "30546231034"},
    {"nombre": "La Sevillanita", "cuit": "30525917404"},
    {"nombre": "Expreso Oro Negro", "cuit": "30525333550"},
    {"nombre": "Raosa Transportes", "cuit": "30518231040"},
    {"nombre": "Transportes Snaider", "cuit": "30554231021"},
    {"nombre": "Expreso Malargue", "cuit": "30519223335"}
]

def load_transportistas():
    with get_db_cursor() as cursor:
        print("--- Cargando Transportistas de Argentina ---")
        for t in TRANSPORTISTAS:
            cursor.execute("SELECT id FROM stk_logisticas WHERE cuit = %s", (t['cuit'],))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO stk_logisticas (enterprise_id, nombre, cuit, activo)
                    VALUES (0, %s, %s, 1)
                """, (t['nombre'], t['cuit']))
                print(f"   [OK] {t['nombre']} agregado.")
            else:
                print(f"   [SKIP] {t['nombre']} ya existe.")
        print("--- Carga finalizada ---")

if __name__ == "__main__":
    load_transportistas()
