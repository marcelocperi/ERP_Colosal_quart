import os
import sys
import json
import logging
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor, DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_indexes():
    db_name = DB_CONFIG.get('database', '')
    
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Obtener todas las tablas
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """, (db_name,))
        tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
        
        # 2. Obtener todas las columnas
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = %s
        """, (db_name,))
        columns_by_table = defaultdict(list)
        for row in cursor.fetchall():
            columns_by_table[row['TABLE_NAME']].append(row['COLUMN_NAME'])

        # 3. Obtener índices existentes
        cursor.execute("""
            SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
        """, (db_name,))
        
        indexes_by_table = defaultdict(lambda: defaultdict(list))
        for row in cursor.fetchall():
            indexes_by_table[row['TABLE_NAME']][row['INDEX_NAME']].append(row['COLUMN_NAME'])

    
    recommendations = []
    
    # Reglas de Tunning heurísticas:
    for table in tables:
        cols = set(columns_by_table[table])
        existing_indices = list(indexes_by_table[table].values())
        
        def has_index_starting_with(col_list):
            for idx_cols in existing_indices:
                # Comprobar si el índice empieza exactamente con estas columnas
                if idx_cols[:len(col_list)] == col_list:
                    return True
            return False

        # Regla 1: Multi-tenant. Toda tabla con enterprise_id de uso frecuente debería tener index 
        # Si tiene enterprise_id + (algún id principal o fecha o estado)
        if 'enterprise_id' in cols:
            # Buscar columnas FK o importantes
            for col in cols:
                if col.endswith('_id') and col not in ('id', 'enterprise_id'):
                    if not has_index_starting_with(['enterprise_id', col]):
                        # A veces el índice está al revés (col, enterprise_id)
                        if not has_index_starting_with([col, 'enterprise_id']):
                            recommendations.append(f"CREATE INDEX idx_{table}_ent_{col.replace('_id','')} ON {table} (enterprise_id, {col});")
            
            # Indexar campos de estado por enterprise
            for col in cols:
                if col in ('estado', 'status', 'activo'):
                    if not has_index_starting_with(['enterprise_id', col]):
                        recommendations.append(f"CREATE INDEX idx_{table}_ent_{col} ON {table} (enterprise_id, {col});")
                        
            # Indexar fechas importantes por enterprise
            for col in cols:
                if col in ('fecha', 'created_at', 'fecha_emision'):
                    if not has_index_starting_with(['enterprise_id', col]):
                        recommendations.append(f"CREATE INDEX idx_{table}_ent_{col} ON {table} (enterprise_id, {col});")

        # Regla 2: FK aislados (tablas sin enterprise_id o además de él)
        for col in cols:
            if col.endswith('_id') and col != 'id':
                if not has_index_starting_with([col]):
                    # Verificar si al menos está en un índice compuesto como segundo
                    in_any = any(col in idx for idx in existing_indices)
                    if not in_any:
                        recommendations.append(f"CREATE INDEX idx_{table}_{col.replace('_id','')} ON {table} ({col});")

        # Regla 3: Columnas de búsqueda frecuente (códigos, emails, documentos)
        for search_col in ['codigo', 'codigo_interno', 'email', 'nro_documento', 'cuit', 'numero', 'numero_serie']:
            if search_col in cols:
                if 'enterprise_id' in cols:
                    if not has_index_starting_with(['enterprise_id', search_col]):
                        recommendations.append(f"CREATE INDEX idx_{table}_{search_col} ON {table} (enterprise_id, {search_col});")
                else:
                    if not has_index_starting_with([search_col]):
                         recommendations.append(f"CREATE INDEX idx_{table}_{search_col} ON {table} ({search_col});")

    print(f"-- Encontradas {len(recommendations)} recomendaciones de inidices --")
    
    # Agrupar por tabla para legibilidad
    recs_by_table = defaultdict(list)
    for r in recommendations:
        # Extraer nombre de tabla del create index
        parts = r.split(' ')
        try:
            tname = parts[4]
            recs_by_table[tname].append(r)
        except:
            pass

    with open('index_recommendations.sql', 'w', encoding='utf-8') as f:
        for tname in sorted(recs_by_table.keys()):
            f.write(f"\n-- Tabla: {tname}\n")
            for r in recs_by_table[tname]:
                f.write(r + "\n")

    print("Guardado en index_recommendations.sql")

if __name__ == '__main__':
    analyze_indexes()
