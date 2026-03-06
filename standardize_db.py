from database import get_db_cursor
import traceback

def fix_all_collations():
    with get_db_cursor() as cursor:
        print("Starting Database Collation Standardization...")
        
        # 1. Disable FK checks temporarily to allow structural changes
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        try:
            # 2. Get all tables in the database
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                print(f"Normalizing table: {table}")
                # This converts the table and ALL its character/string columns to the standard collation
                try:
                    cursor.execute(f"ALTER TABLE `{table}` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                except Exception as e:
                    print(f"  Warning: Could not convert table {table}: {e}")

            # 3. Also set the database default collation
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            cursor.execute(f"ALTER DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            print("\nDatabase standardized to utf8mb4_unicode_ci.")
            
        except Exception:
            print("Error during conversion:")
            traceback.print_exc()
        finally:
            # 4. Re-enable FK checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            print("Foreign key checks re-enabled.")

if __name__ == "__main__":
    fix_all_collations()
