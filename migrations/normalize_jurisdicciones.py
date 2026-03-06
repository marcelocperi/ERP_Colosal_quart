
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

def run():
    print("🚀 Creating 'sys_jurisdicciones_iibb' and normalizing padron table...")
    
    with get_db_cursor() as cursor:
        # 1. Create Jurisdicciones Table (AFIP Codes)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_jurisdicciones_iibb (
                codigo INT PRIMARY KEY, -- Codigo AFIP (901, 902, etc.)
                nombre VARCHAR(100) NOT NULL,
                alias VARCHAR(50), -- ARBA, AGIP, API, RENTAS_CBA
                activo TINYINT(1) DEFAULT 1
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 2. Seed Jurisdicciones
        jurisdicciones = [
            (901, 'Ciudad Autónoma de Buenos Aires', 'AGIP'),
            (902, 'Buenos Aires', 'ARBA'),
            (903, 'Catamarca', 'Rentas Catamarca'),
            (904, 'Córdoba', 'Rentas Córdoba'),
            (905, 'Corrientes', 'Rentas Corrientes'),
            (906, 'Chaco', 'ATP Chaco'),
            (907, 'Chubut', 'Rentas Chubut'),
            (908, 'Entre Ríos', 'ATER'),
            (909, 'Formosa', 'Rentas Formosa'),
            (910, 'Jujuy', 'Rentas Jujuy'),
            (911, 'La Pampa', 'Rentas La Pampa'),
            (912, 'La Rioja', 'Rentas La Rioja'),
            (913, 'Mendoza', 'ATM Mendoza'),
            (914, 'Misiones', 'Rentas Misiones'),
            (915, 'Neuquén', 'Rentas Neuquén'),
            (916, 'Río Negro', 'Rentas Río Negro'),
            (917, 'Salta', 'Rentas Salta'),
            (918, 'San Juan', 'Rentas San Juan'),
            (919, 'San Luis', 'Rentas San Luis'),
            (920, 'Santa Cruz', 'ASIP Santa Cruz'),
            (921, 'Santa Fe', 'API Santa Fe'),
            (922, 'Santiago del Estero', 'Rentas Santiago'),
            (923, 'Tucumán', 'Rentas Tucumán'),
            (924, 'Tierra del Fuego', 'AREF TDF')
        ]
        
        for j in jurisdicciones:
            cursor.execute("""
                INSERT INTO sys_jurisdicciones_iibb (codigo, nombre, alias, activo)
                VALUES (%s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE nombre = VALUES(nombre), alias = VALUES(alias)
            """, j)
            
        print("✅ Jurisdicciones seeded.")
        
        # 3. Add jurisdiccion_id to sys_padrones_iibb
        # First check if column exists
        cursor.execute("SHOW COLUMNS FROM sys_padrones_iibb LIKE 'jurisdiccion_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE sys_padrones_iibb ADD COLUMN jurisdiccion_id INT DEFAULT NULL AFTER id")
            cursor.execute("ALTER TABLE sys_padrones_iibb ADD INDEX (jurisdiccion_id)")
            print("✅ Added 'jurisdiccion_id' column to sys_padrones_iibb.")
        
        # 4. Migrate Data
        print("🔄 Migrating existing data...")
        cursor.execute("UPDATE sys_padrones_iibb SET jurisdiccion_id = 901 WHERE jurisdiccion = 'AGIP'")
        cursor.execute("UPDATE sys_padrones_iibb SET jurisdiccion_id = 902 WHERE jurisdiccion = 'ARBA'")
        cursor.execute("UPDATE sys_padrones_iibb SET jurisdiccion_id = 921 WHERE jurisdiccion = 'SANTA_FE'")
        cursor.execute("UPDATE sys_padrones_iibb SET jurisdiccion_id = 904 WHERE jurisdiccion = 'CORDOBA'")
        
        # 5. Make sys_padrones_logs conform if needed (optional, keeping string for logs is fine for readability)
        
if __name__ == "__main__":
    run()
