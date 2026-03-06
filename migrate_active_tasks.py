
from database import get_db_cursor

def migrate():
    try:
        with get_db_cursor() as cursor:
            print("Updating sys_active_tasks table...")
            
            # Add status column
            try:
                cursor.execute("ALTER TABLE sys_active_tasks ADD COLUMN status VARCHAR(20) DEFAULT 'RUNNING'")
                print("- Added 'status' column")
            except Exception as e:
                print(f"- 'status' column might already exist: {e}")

            # Add requested_stop column
            try:
                cursor.execute("ALTER TABLE sys_active_tasks ADD COLUMN requested_stop BOOLEAN DEFAULT 0")
                print("- Added 'requested_stop' column")
            except Exception as e:
                print(f"- 'requested_stop' column might already exist: {e}")

            # Add source_origin column
            try:
                cursor.execute("ALTER TABLE sys_active_tasks ADD COLUMN source_origin VARCHAR(20) DEFAULT 'WEB'")
                print("- Added 'source_origin' column")
            except Exception as e:
                print(f"- 'source_origin' column might already exist: {e}")
            
            print("Migration completed successfully.")
            
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate()
