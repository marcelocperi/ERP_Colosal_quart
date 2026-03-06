
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

def run():
    print("🚀 Adding AFIP columns to sys_enterprises...")
    
    with get_db_cursor() as cursor:
        # Check if columns exist
        cursor.execute("SHOW COLUMNS FROM sys_enterprises")
        cols = [c[0] for c in cursor.fetchall()]
        
        needed = {
            'afip_crt': 'TEXT',
            'afip_key': 'TEXT',
            'afip_entorno': "VARCHAR(20) DEFAULT 'testing'",
            'afip_puesto': "INT DEFAULT 1"
        }
        
        for col, col_type in needed.items():
            if col not in cols:
                cursor.execute(f"ALTER TABLE sys_enterprises ADD COLUMN {col} {col_type}")
                print(f"✅ Added column: {col}")
        
    print("✅ AFIP columns verified.")

if __name__ == "__main__":
    run()
