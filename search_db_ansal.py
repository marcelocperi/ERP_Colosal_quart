import mariadb
from database import DB_CONFIG

def search_db():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SHOW TABLES")
    tables = [t[0] for t in cursor.fetchall()]
    
    found = False
    for table in tables:
        try:
            cursor.execute(f"DESCRIBE `{table}`")
            columns = cursor.fetchall()
            # Look for string columns
            char_cols = [c[0] for c in columns if any(typ in c[1].lower() for typ in ['varchar', 'text', 'char', 'json'])]
            
            if not char_cols:
                continue
                
            conditions = [f"`{col}` LIKE '%ansal%'" for col in char_cols]
            query = f"SELECT * FROM `{table}` WHERE " + " OR ".join(conditions)
            
            cursor.execute(query)
            results = cursor.fetchall()
            if results:
                print(f"Found in table '{table}':")
                for r in results:
                    print(f"  {r}")
                found = True
        except Exception as e:
            # print(f"Error searching table {table}: {e}")
            pass
            
    if not found:
        print("No occurrences of 'ansal' found in the database.")
        
    conn.close()

if __name__ == "__main__":
    search_db()
