from database import get_db_cursor
import os

def run_sql(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by semicolon but join if it's not a complete statement? 
    # Usually splitting by semicolon and stripping is enough if there are no semicolons in literals.
    statements = [s.strip() for s in content.split(';') if s.strip()]
    
    with get_db_cursor() as cursor:
        print(f"Executing {len(statements)} statements de {filename}...")
        for s in statements:
            try:
                # Remove comments
                clean_lines = [l for l in s.split('\n') if not l.strip().startswith('--')]
                clean_s = ' '.join(clean_lines).strip()
                if clean_s:
                    cursor.execute(clean_s)
            except Exception as e:
                print(f"Error: {e} \nStmt: {s[:100]}...")

if __name__ == "__main__":
    run_sql('migrations/msac_phase_1_4_y_1_5.sql')
    run_sql('migrations/msac_phase_1_6_repositorio_documental.sql')
