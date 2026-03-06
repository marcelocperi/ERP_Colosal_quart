"""
python_progs_vs_db_schema.py  v2
=================================
Deep Research: Cruza todas las sentencias SQL del código fuente Python
contra el schema real de la base de datos.

Mejoras v2:
  - Filtro de nombres de tabla: solo acepta tablas con guiones bajos (patrón DB real)
    o que ya existan en la DB. Elimina falsos positivos tipo 'con', 'para', 'flask'.
  - Limpieza robusta de SQL multilínea antes del parse (evita 'nterprise_id').
  - Columnas mínimo 3 chars y deben ser identificadores completos.
  - Excluye scripts de utilidad, test, debug, check, migrate, setup.
  - Archivos de código productivo priorizados.
"""

import os
import re
import json
from datetime import datetime
from collections import defaultdict
from database import get_db_cursor, DB_CONFIG

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "schema_check_report.json")

# Directorios a ignorar
EXCLUDE_DIRS = {
    'venv', '.git', '__pycache__', '.agent', 'node_modules',
    'migrations', 'tmp', 'docs', 'tests', 'test',
}

# Archivos de utilidad / debug / setup — no son código productivo
EXCLUDE_FILE_PREFIXES = (
    'check_', 'debug_', 'setup_', 'migrate_', 'seed_',
    'fix_', 'ensure_', 'reset_', 'inspect_', 'configure_',
    'add_', 'modify_', 'test_', 'tmp_',
)
EXCLUDE_FILES_EXACT = {
    'python_progs_vs_db_schema.py',
    'python_check_remediate.py',
    '_md_to_docx.py',
}

# Módulos productivos dónde sí importa la precisión
PRODUCTIVE_DIRS = {'core', 'compras', 'ventas', 'stock', 'fondos',
                   'contabilidad', 'produccion', 'biblioteca', 'services',
                   'pricing', 'utilitarios', 'utils'}

# SQL keywords y Python/Flask common names que se confunden con tablas
SQL_KEYWORDS = {
    'select', 'from', 'where', 'and', 'or', 'not', 'in', 'is', 'null',
    'like', 'between', 'order', 'by', 'group', 'having', 'limit', 'offset',
    'join', 'left', 'right', 'inner', 'outer', 'on', 'as', 'distinct',
    'count', 'sum', 'max', 'min', 'avg', 'coalesce', 'ifnull', 'isnull',
    'case', 'when', 'then', 'else', 'end', 'insert', 'into', 'values',
    'update', 'set', 'delete', 'create', 'drop', 'alter', 'table', 'index',
    'view', 'returning', 'with', 'union', 'all', 'exists', 'any', 'some',
    'asc', 'desc', 'true', 'false', 'if', 'for', 'show', 'columns',
    'describe', 'explain', 'use', 'database', 'schema', 'at', 'interval',
    'date', 'time', 'year', 'month', 'day', 'now', 'curdate', 'concat',
    'length', 'substring', 'trim', 'upper', 'lower', 'replace', 'round',
    'floor', 'ceil', 'abs', 'mod', 'cast', 'convert', 'char', 'varchar',
    'int', 'bigint', 'float', 'decimal', 'text', 'blob', 'json', 'bool',
    'datetime', 'timestamp',
}

# Nombres que NO son tablas de DB (variables Python, alias SQL comunes)
NOT_A_TABLE = {
    'con', 'cur', 'cursor', 'conn', 'connection', 'db', 'para',
    'flask', 'services', 'result', 'results', 'row', 'rows', 'data',
    'item', 'items', 'val', 'value', 'values', 'res', 'tmp', 'temp',
    'subq', 'sub', 'q', 'query', 'sql', 'stmt', 't', 'u', 'r', 'p',
    'information_schema', 'performance_schema', 'mysql', 'sys',
    'source_ids',  # variable Python mal capturada
    'libros',      # tabla que fue renombrada / esquema diferente
    'permissions', 'roles_permissions',  # nombres genéricos no de este esquema
    'core_roles', 'core_permisos', 'core_rol_permisos',  # esquema viejo
    'flask', 'loancounts', 'pendingcounts', 'encaminocounts',
}


# ─── PASO 1: Cargar DDL de la DB en batch ──────────────────────────────────────

def load_db_schema():
    print("📥 Cargando schema de la base de datos (batch)...")
    schema = defaultdict(set)
    db_name = DB_CONFIG.get('database', '')

    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """, (db_name,))
        for row in cursor.fetchall():
            schema[row['TABLE_NAME'].lower()].add(row['COLUMN_NAME'].lower())

    print(f"   ✅ {len(schema)} tablas, {sum(len(v) for v in schema.values())} columnas cargadas.")
    return schema


# ─── PASO 2: Seleccionar archivos ──────────────────────────────────────────────

def get_py_files():
    py_files = []
    for root, dirs, files in os.walk(BASE_DIR):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            if not f.endswith('.py'):
                continue
            if f in EXCLUDE_FILES_EXACT:
                continue
            if any(f.startswith(p) for p in EXCLUDE_FILE_PREFIXES):
                continue
            py_files.append(os.path.join(root, f))
    return py_files


def is_productive_file(fpath):
    """True si el archivo está en un módulo productivo."""
    rel = os.path.relpath(fpath, BASE_DIR).replace('\\', '/')
    first_dir = rel.split('/')[0]
    return first_dir in PRODUCTIVE_DIRS


# ─── PASO 3: Extraer y limpiar SQL ─────────────────────────────────────────────

def clean_sql(raw):
    """
    Normaliza un fragmento SQL:
    - Colapsa whitespace/newlines en un espacio
    - Elimina comentarios -- y /* */
    - Elimina f-string residuos: {var}
    """
    # Quitar comentarios bloque
    raw = re.sub(r'/\*.*?\*/', ' ', raw, flags=re.DOTALL)
    # Quitar comentarios línea
    raw = re.sub(r'--[^\n]*', ' ', raw)
    # Quitar placeholders f-string: {algo}
    raw = re.sub(r'\{[^}]{1,40}\}', ' %s ', raw)
    # Colapsar whitespace
    raw = re.sub(r'\s+', ' ', raw).strip()
    return raw


def extract_sql_strings(content):
    """Extrae strings Python que contienen SQL."""
    sql_frags = []
    SQL_SIGNAL = re.compile(
        r'\b(SELECT|INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM|FROM\s+\w)\b',
        re.IGNORECASE
    )

    # Triple-quoted
    for pat in (r'"""(.*?)"""', r"'''(.*?)'''"):
        for m in re.finditer(pat, content, re.DOTALL):
            frag = m.group(1)
            if SQL_SIGNAL.search(frag):
                sql_frags.append((clean_sql(frag), m.start()))

    # Single/double quoted (mínimo 25 chars para evitar strings cortos)
    for pat in (r'"((?:[^"\\]|\\.){25,})"', r"'((?:[^'\\]|\\.){25,})'"):
        for m in re.finditer(pat, content, re.DOTALL):
            frag = m.group(1)
            if SQL_SIGNAL.search(frag):
                sql_frags.append((clean_sql(frag), m.start()))

    return sql_frags


# ─── PASO 4: Parsear tabla + columnas ──────────────────────────────────────────

def is_valid_table_name(name, db_schema):
    """
    Acepta un nombre como tabla si:
    - Tiene al menos un guión bajo (convención del proyecto: sys_, cmp_, stk_...)
    - O ya existe en el schema de la DB
    - Y NO está en la lista de exclusiones
    """
    if name in NOT_A_TABLE or name in SQL_KEYWORDS:
        return False
    if len(name) < 4:
        return False
    # Debe tener guión bajo (convención) o existir en la DB
    return '_' in name or name in db_schema


def is_valid_column(col):
    """Columna válida: mínimo 3 chars, no keyword, no empieza con número."""
    if col in SQL_KEYWORDS:
        return False
    if len(col) < 3:
        return False
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
        return False
    return True


def parse_sql(sql, db_schema):
    """Extrae pares (stmt_type, table, [columns]) del SQL."""
    results = []

    # ── INSERT INTO table (cols) ──────────────────────────────────────────
    for m in re.finditer(
        r'\bINSERT\s+INTO\s+`?(\w+)`?\s*\(([^)]+)\)',
        sql, re.IGNORECASE
    ):
        table = m.group(1).lower()
        if not is_valid_table_name(table, db_schema):
            continue
        cols = [c.strip().strip('`\'" ').lower() for c in m.group(2).split(',')]
        cols = [c for c in cols if is_valid_column(c)]
        results.append(('INSERT', table, cols))

    # ── UPDATE table SET col=val ... ──────────────────────────────────────
    for m in re.finditer(
        r'\bUPDATE\s+`?(\w+)`?\s+SET\s+(.+?)(?:\bWHERE\b|$)',
        sql, re.IGNORECASE
    ):
        table = m.group(1).lower()
        if not is_valid_table_name(table, db_schema):
            continue
        cols = re.findall(r'`?([a-zA-Z_]\w*)`?\s*=', m.group(2))
        cols = [c.lower() for c in cols if is_valid_column(c)]
        results.append(('UPDATE', table, cols))

    # ── SELECT cols FROM table ────────────────────────────────────────────
    for m in re.finditer(
        r'\bSELECT\b(.*?)\bFROM\b\s+`?(\w+)`?',
        sql, re.IGNORECASE
    ):
        table = m.group(2).lower()
        if not is_valid_table_name(table, db_schema):
            continue
        select_part = m.group(1)
        if '*' in select_part:
            results.append(('SELECT*', table, []))
            continue
        # Quitar funciones: FUNC(...)
        select_clean = re.sub(r'\b\w+\s*\([^)]*\)', '', select_part)
        # Solo capturar col sin alias.col (evitar alias como prefijo)
        cols = re.findall(r'(?<!\w\.)(?<![.\w])([a-zA-Z_]\w*)(?!\s*\()', select_clean)
        cols = [c.lower() for c in cols if is_valid_column(c)]
        results.append(('SELECT', table, cols))

    # ── FROM / JOIN (solo tablas, sin columnas) ───────────────────────────
    for m in re.finditer(
        r'\b(?:FROM|JOIN)\s+`?(\w+)`?(?:\s+(?:AS\s+)?\w+)?(?:\s+ON\b|\s+WHERE\b|\s*$|\s*,)',
        sql, re.IGNORECASE
    ):
        table = m.group(1).lower()
        if is_valid_table_name(table, db_schema):
            results.append(('FROM', table, []))

    # ── WHERE / AND col op val (asociado a tabla primaria FROM) ──────────
    from_m = re.search(r'\bFROM\s+`?(\w+)`?', sql, re.IGNORECASE)
    if from_m:
        primary = from_m.group(1).lower()
        if is_valid_table_name(primary, db_schema):
            where_m = re.search(r'\bWHERE\b(.+?)(?:\bORDER\b|\bGROUP\b|\bLIMIT\b|$)',
                                 sql, re.IGNORECASE)
            if where_m:
                where_part = where_m.group(1)
                # Solo capturar col sin prefijo alias.col
                w_cols = re.findall(
                    r'(?<![.\w])([a-zA-Z_]\w*)\s*(?:=|!=|<>|>=|<=|>|<|\bLIKE\b|\bIN\b|\bIS\b)',
                    where_part, re.IGNORECASE
                )
                w_cols = [c.lower() for c in w_cols if is_valid_column(c)]
                if w_cols:
                    results.append(('WHERE', primary, w_cols))

    return results


# ─── PASO 5: Escanear codebase ─────────────────────────────────────────────────

def scan_codebase(db_schema):
    print("🔍 Escaneando código fuente...")
    py_files = get_py_files()
    productive = [f for f in py_files if is_productive_file(f)]
    other      = [f for f in py_files if not is_productive_file(f)]
    print(f"   📂 {len(py_files)} archivos ({len(productive)} productivos, {len(other)} otros)")

    refs         = defaultdict(list)   # (table,col) -> [(file, line, type)]
    tables_found = defaultdict(list)   # table       -> [(file, line, type)]

    for fpath in py_files:
        rel = os.path.relpath(fpath, BASE_DIR)
        is_prod = is_productive_file(fpath)
        try:
            with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception:
            continue

        for sql_frag, pos in extract_sql_strings(content):
            line_no = content[:pos].count('\n') + 1
            for stmt_type, table, cols in parse_sql(sql_frag, db_schema):
                tables_found[table].append((rel, line_no, stmt_type, is_prod))
                for col in cols:
                    refs[(table, col)].append((rel, line_no, stmt_type, is_prod))

    print(f"   ✅ {len(tables_found)} tablas únicas, {len(refs)} refs (tabla,col) únicas.")
    return refs, tables_found


# ─── PASO 6: Cruzar ────────────────────────────────────────────────────────────

def cross_check(db_schema, code_refs, code_tables):
    missing_tables  = []
    missing_columns = []
    ok_tables       = set()
    ok_columns      = set()

    for table, locs in code_tables.items():
        if table not in db_schema:
            missing_tables.append({'table': table, 'locations': locs[:5]})
        else:
            ok_tables.add(table)

    for (table, col), locs in code_refs.items():
        if table not in db_schema:
            continue  # ya reportado
        if col not in db_schema[table]:
            missing_columns.append({
                'table': table,
                'column': col,
                'locations': locs[:5],
                'productive': any(l[3] for l in locs),
            })
        else:
            ok_columns.add((table, col))

    # Ordenar: primero los de archivos productivos
    missing_columns.sort(key=lambda x: (not x['productive'], x['table'], x['column']))
    missing_tables.sort(key=lambda x: x['table'])
    return missing_tables, missing_columns, ok_tables, ok_columns


# ─── PASO 7: Reporte ───────────────────────────────────────────────────────────

def save_and_print(missing_tables, missing_columns, ok_tables, ok_columns, db_schema, code_tables):
    report = {
        'generated_at': datetime.now().isoformat(),
        'version': '2.0',
        'summary': {
            'db_tables': len(db_schema),
            'code_tables_referenced': len(code_tables),
            'missing_tables': len(missing_tables),
            'missing_columns': len(missing_columns),
            'missing_columns_productive': sum(1 for c in missing_columns if c['productive']),
            'ok_tables': len(ok_tables),
            'ok_columns': len(ok_columns),
        },
        'missing_tables':  missing_tables,
        'missing_columns': missing_columns,
    }
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    s = report['summary']
    sep = "=" * 72
    print(f"\n{sep}")
    print("  RESUMEN  |  python_progs_vs_db_schema v2")
    print(sep)
    print(f"  DB: {s['db_tables']} tablas | Código: {s['code_tables_referenced']} tablas referenciadas")
    print(f"  ❌ Tablas faltantes en DB  : {s['missing_tables']}")
    print(f"  ❌ Columnas faltantes en DB: {s['missing_columns']}  "
          f"(⚡ en módulos productivos: {s['missing_columns_productive']})")
    print(f"  ✅ Tablas OK:   {s['ok_tables']}  |  Columnas OK: {s['ok_columns']}")
    print(sep)

    if missing_tables:
        print("\n🔴 TABLAS EN CÓDIGO QUE NO EXISTEN EN DB:")
        for item in missing_tables:
            prod_locs = [l for l in item['locations'] if l[3]]
            locs = prod_locs or item['locations']
            loc_str = "; ".join(f"{l[0]}:L{l[1]}" for l in locs[:3])
            print(f"   {'⚡' if prod_locs else '  '} {item['table']:<30} {loc_str}")

    if missing_columns:
        print("\n🟠 COLUMNAS EN CÓDIGO QUE NO EXISTEN EN SU TABLA DB:")
        print(f"   {'⚡'} = en módulo productivo\n")
        for item in missing_columns[:40]:
            prod_locs = [l for l in item['locations'] if l[3]]
            locs = prod_locs or item['locations']
            loc_str = "; ".join(f"{l[0]}:L{l[1]}" for l in locs[:2])
            marker = '⚡' if item['productive'] else '  '
            print(f"   {marker} {item['table']:<28}.{item['column']:<22} {loc_str}")
        if len(missing_columns) > 40:
            print(f"   ... y {len(missing_columns)-40} más → ver schema_check_report.json")

    if not missing_tables and not missing_columns:
        print("\n  🎉 Sin inconsistencias. El código está alineado con la DB.")

    print(f"\n💾 Reporte completo: schema_check_report.json\n{sep}\n")
    return report


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 72)
    print("  python_progs_vs_db_schema.py  v2  |  Deep Research: Código vs DB")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72 + "\n")

    db_schema = load_db_schema()
    code_refs, code_tables = scan_codebase(db_schema)
    print("\n🔄 Cruzando referencias...")
    mt, mc, ot, oc = cross_check(db_schema, code_refs, code_tables)
    save_and_print(mt, mc, ot, oc, db_schema, code_tables)


if __name__ == "__main__":
    main()
