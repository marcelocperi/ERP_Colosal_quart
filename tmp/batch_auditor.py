
import sqlite3
import os
import re

DB_PATH = 'multi_mcp.db'
RULES_DIR = os.path.join('.brain', 'rules')
DEBT_FILE = os.path.join(RULES_DIR, 'current_debt.md')

AUDIT_FIELDS = ['user_id', 'created_at', 'user_id_update', 'updated_at']

def audit_database():
    print("Auditing Database Schema...")
    violations = []
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]
        
        for table in tables:
            if table.startswith('sqlite_'): continue
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]
            missing = [field for field in AUDIT_FIELDS if field not in columns]
            if missing:
                violations.append(f"- **Tabla `{table}`**: Faltan campos: {', '.join(missing)}")
        conn.close()
    except Exception as e:
        print(f"Error auditing DB: {e}")
    return violations

def audit_code():
    print("Auditing Source Code (Retroactive Scan)...")
    code_violations = []
    # Extensiones a buscar
    search_dirs = ['core', 'compras', 'stock', 'biblioteca', 'ventas', 'fondos']
    
    insert_pattern = re.compile(r"INSERT\s+INTO\s+(\w+)", re.IGNORECASE)
    
    for sdir in search_dirs:
        if not os.path.exists(sdir): continue
        for root, _, files in os.walk(sdir):
            for file in files:
                if file.endswith('.py'):
                    path = os.path.join(root, file)
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        # Buscar si hay inserts que no mencionen los campos de auditoría
                        matches = insert_pattern.findall(content)
                        for match in set(matches):
                            # Si el archivo tiene un INSERT pero no menciona user_id en el mismo bloque o archivo
                            # (Simplificación: si no aparece 'user_id' en el archivo, probablemente es deuda)
                            if 'user_id' not in content:
                                code_violations.append(f"- **Archivo `{path}`**: Usa `INSERT` en `{match}` pero no parece gestionar `user_id`.")
                                break # No repetir por archivo
    return code_violations

def run_batch_audit():
    db_violations = audit_database()
    code_violations = audit_code()
    
    report = "# Reporte de Deuda Técnica - Auditoría Retroactiva\n\n"
    report += "Este documento alimenta el contexto del LLM local con el estado actual de incumplimientos.\n\n"
    
    report += "## 1. Incumplimientos de Base de Datos (Esquema)\n"
    if db_violations:
        report += "\n".join(db_violations)
    else:
        report += "¡Felicidades! Todas las tablas cumplen con el esquema de trazabilidad."
        
    report += "\n\n## 2. Sospechas de Deuda en Código Fuente\n"
    if code_violations:
        report += "\n".join(code_violations)
    else:
        report += "No se detectaron patrones de inserción sin auditoría."
        
    report += "\n\n--- \n*Generado automáticamente por BatchAuditorProcessor*"
    
    if not os.path.exists(RULES_DIR):
        os.makedirs(RULES_DIR)
        
    with open(DEBT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)
        
    print(f"Reporte generado en {DEBT_FILE}. El LLM ahora conoce esta deuda.")

if __name__ == "__main__":
    run_batch_audit()
