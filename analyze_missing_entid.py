from database import get_db_cursor

def analyze_schema():
    print("Checking for missing enterprise_id in tables...\n")
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [r[0] for r in cursor.fetchall()]
        
        missing = []
        has_it = []
        
        for table in tables:
            cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            cols = [r[0].lower() for r in cursor.fetchall()]
            
            if 'enterprise_id' in cols:
                has_it.append(table)
            else:
                missing.append(table)
        
        print(f"--- TABLES WITH enterprise_id ({len(has_it)}) ---")
        # print(", ".join(has_it))
        print("\n--- TABLES MISSING enterprise_id ({len(missing)}) ---")
        for m in missing:
            # Get a sample row or comment to guess function
            print(f"- {m}")

if __name__ == "__main__":
    analyze_schema()
