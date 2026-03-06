"""
Seed de bancos CBU argentinos con códigos oficiales del BCRA.
Fuente: Comunicación BCRA A 6268 y registro de entidades financieras.
Se usa cuando la API del BCRA no está disponible.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from database import get_db_cursor
from services.bcra_service import BCRAService

# Listado oficial de entidades bancarias argentinas
# Formato: (bcra_id, nombre, tipo, codigo_cbu_3d)
BANCOS_SEED = [
    # ── Bancos Públicos ──────────────────────────────────────────────────────
    (11,  "Banco de la Nación Argentina",            "Banco Público",   "011"),
    (14,  "Banco de la Provincia de Buenos Aires",   "Banco Público",   "014"),
    (20,  "Banco de la Provincia de Córdoba",        "Banco Público",   "020"),
    (29,  "Banco de la Ciudad de Buenos Aires",      "Banco Público",   "029"),
    (45,  "Banco de San Juan",                       "Banco Público",   "045"),
    (46,  "Banco de la Nación Argentina (Sucursal)", "Banco Público",   "046"),
    (65,  "Banco Municipal de Rosario",              "Banco Público",   "065"),
    (86,  "Banco del Chubut",                        "Banco Público",   "086"),
    (94,  "Banco de la Pampa",                       "Banco Público",   "094"),
    (97,  "Banco del Chaco",                         "Banco Público",   "097"),
    (98,  "Banco de Formosa",                        "Banco Público",   "098"),
    (268, "Banco de Inversión y Comercio Exterior",  "Banco Público",   "268"),
    (384, "Banco de la Rioja",                       "Banco Público",   "384"),

    # ── Bancos Privados Principales ──────────────────────────────────────────
    (7,   "Banco Galicia",                           "Banco Comercial", "007"),
    (8,   "Banco Credicoop Cooperativo",             "Banco Comercial", "008"),
    (15,  "Banco HSBC Argentina",                    "Banco Comercial", "015"),
    (16,  "Citibank N.A.",                           "Banco Comercial", "016"),
    (17,  "BBVA Argentina",                          "Banco Comercial", "017"),
    (24,  "Banco Macro",                             "Banco Comercial", "024"),
    (27,  "Banco Supervielle",                       "Banco Comercial", "027"),
    (44,  "Banco Patagonia",                         "Banco Comercial", "044"),
    (60,  "Banco de Tucumán",                        "Banco Comercial", "060"),
    (72,  "Banco Santander Argentina",               "Banco Comercial", "072"),
    (75,  "Banco Itaú Argentina",                    "Banco Comercial", "075"),
    (83,  "Banco Comafi",                            "Banco Comercial", "083"),
    (93,  "ICBC (Industrial and Commercial Bank)",   "Banco Comercial", "093"),
    (147, "Banco Bica",                              "Banco Comercial", "147"),
    (150, "Banco de Servicios y Transacciones",      "Banco Comercial", "150"),
    (165, "Banco CMF",                               "Banco Comercial", "165"),
    (191, "Brubank",                                 "Banco Comercial", "191"),
    (198, "Banco de Valores",                        "Banco Comercial", "198"),
    (247, "Banco Hipotecario",                       "Banco Hipotecario","247"),
    (254, "Banco Voii",                              "Banco Comercial", "254"),
    (259, "Banco Piano",                             "Banco Comercial", "259"),
    (266, "Banco Mariva",                            "Banco Comercial", "266"),
    (270, "Banco Roela",                             "Banco Comercial", "270"),
    (277, "Banco Meridian",                          "Banco Comercial", "277"),
    (281, "Banco Industrial",                        "Banco Comercial", "281"),
    (285, "Deutsche Bank",                           "Banco Comercial", "285"),
    (295, "Banco de Comercio",                       "Banco Comercial", "295"),
    (300, "Banco Sucredito Regional",                "Banco Comercial", "300"),
    (310, "Banco Masventas",                         "Banco Comercial", "310"),
    (311, "Wilobank",                                "Banco Comercial", "311"),
    (315, "Reba Compañía Financiera",                "Compañía Financiera","315"),
    (319, "Naranja X",                               "Compañía Financiera","319"),
    (321, "Finandí Compañía Financiera",             "Compañía Financiera","321"),
    (322, "Crédito Regional Compañía Financiera",    "Compañía Financiera","322"),
    (330, "Cordial Compañía Financiera",             "Compañía Financiera","330"),
    (336, "RCI Banque",                              "Compañía Financiera","336"),
    (338, "PSA Finance Argentina",                   "Compañía Financiera","338"),
    (339, "Toyota Compañía Financiera",              "Compañía Financiera","339"),
    (340, "FCA Compañía Financiera",                 "Compañía Financiera","340"),
    (341, "Volkswagen Financial Services",           "Compañía Financiera","341"),
    (384, "Mercado Crédito Compañía Financiera",     "Compañía Financiera","382"),
]

print(f"Cargando {len(BANCOS_SEED)} bancos/entidades CBU en fin_bancos...")

inserted = 0
updated  = 0
cuentas  = 0

with get_db_cursor() as cursor:
    for bcra_id, nombre, tipo, codigo_cbu in BANCOS_SEED:
        # Crear/obtener cuenta analítica 1.1.02.XXX
        cuenta_id = BCRAService._get_o_crear_cuenta(
            cursor, 0, 'CBU', codigo_cbu, nombre)
        if cuenta_id:
            cuentas += 1

        cursor.execute("""
            INSERT INTO fin_bancos
                (enterprise_id, bcra_id, tipo_entidad, codigo_cbu,
                 cuenta_contable_id, nombre, tipo, origen, activo)
            VALUES (0, %s, 'CBU', %s, %s, %s, %s, 'BCRA', 1)
            ON DUPLICATE KEY UPDATE
                nombre             = VALUES(nombre),
                tipo               = VALUES(tipo),
                tipo_entidad       = 'CBU',
                codigo_cbu         = COALESCE(VALUES(codigo_cbu), codigo_cbu),
                cuenta_contable_id = COALESCE(VALUES(cuenta_contable_id), cuenta_contable_id),
                activo             = 1,
                updated_at         = CURRENT_TIMESTAMP
        """, (bcra_id, codigo_cbu, cuenta_id, nombre, tipo))

        if cursor.rowcount == 1:
            inserted += 1
        else:
            updated += 1

print(f"\nResultado:")
print(f"  Insertados     : {inserted}")
print(f"  Actualizados   : {updated}")
print(f"  Cuentas creadas: {cuentas}")

# Verificación final
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("""
        SELECT b.tipo_entidad, COUNT(*) AS total,
               SUM(b.cuenta_contable_id IS NOT NULL) AS con_cuenta
        FROM fin_bancos b
        GROUP BY b.tipo_entidad
    """)
    print("\nResumen fin_bancos:")
    for r in cursor.fetchall():
        print(f"  {r['tipo_entidad']}: {r['total']} entidades, {r['con_cuenta']} con cuenta contable")

print("\nDetalle CBU (primeros 15):")
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("""
        SELECT b.codigo_cbu, b.nombre, c.codigo AS cuenta
        FROM fin_bancos b
        LEFT JOIN cont_plan_cuentas c ON b.cuenta_contable_id = c.id
        WHERE b.tipo_entidad = 'CBU'
        ORDER BY b.codigo_cbu
        LIMIT 15
    """)
    for r in cursor.fetchall():
        print(f"  {r['codigo_cbu']}  {r['nombre']:<45} -> {r['cuenta'] or 'sin cuenta'}")
