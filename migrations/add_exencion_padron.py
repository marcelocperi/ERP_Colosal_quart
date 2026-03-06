
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

def run():
    print("🚀 Adding 'exencion_iibb' to sys_padrones_iibb...")
    
    with get_db_cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM sys_padrones_iibb LIKE 'exencion_iibb'")
        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE sys_padrones_iibb 
                ADD COLUMN exencion_iibb VARCHAR(20) DEFAULT '' COMMENT 'EXENTO, NO_EXENTO, etc.'
            """)
            print("  ✅ Column 'exencion_iibb' added.")
        else:
            print("  ℹ️  Column 'exencion_iibb' already exists.")

if __name__ == '__main__':
    run()
