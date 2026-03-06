import os
import sys
import logging
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor, DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def advanced_index_tuning():
    db_name = DB_CONFIG.get('database', '')
    
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Obtenemos las tablas y su cardinalidad estimada
        cursor.execute("""
            SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """, (db_name,))
        table_stats = {row['TABLE_NAME']: row for row in cursor.fetchall()}
        tables = list(table_stats.keys())

        # 2. Obtenemos definiciones de columnas (Buscamos FK implícitas, Fechas y Filtros)
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_KEY
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = %s
        """, (db_name,))
        columns_by_table = defaultdict(dict)
        for row in cursor.fetchall():
            columns_by_table[row['TABLE_NAME']][row['COLUMN_NAME']] = {
                'type': row['DATA_TYPE'],
                'key': row['COLUMN_KEY']
            }

        # 3. Obtenemos índices existentes y sus columnas para evitar duplicados
        cursor.execute("""
            SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
        """, (db_name,))
        
        indexes_db = defaultdict(lambda: defaultdict(list))
        for row in cursor.fetchall():
            indexes_db[row['TABLE_NAME']][row['INDEX_NAME']].append(row['COLUMN_NAME'])

    recommendations = []
    
    # helper: verifica si existe un índice en la tabla que cubra secuencialmente las columnas
    def has_covering_index(table, cols):
        existing = indexes_db.get(table, {})
        for idx_name, idx_cols in existing.items():
            if len(idx_cols) < len(cols):
                continue
            # Verifica prefijo
            if idx_cols[:len(cols)] == cols:
                return True
        return False

    # TABLAS DE ALTO TRÁFICO/OPERACIONALES A PRIORIZAR
    # Aquellas con prefijos: cmp_ (compras), vtas_ (ventas), stk_ (stock), fondo_, prod_
    core_prefixes = ('cmp_', 'vtas_', 'stk_', 'fondo_', 'prod_', 'erp_')

    for table in tables:
        stats = table_stats[table]
        cols = columns_by_table[table]
        
        # Filtros de Tunning Experto
        has_ent = 'enterprise_id' in cols
        
        # 1. Estrategia Multi-Tenant: Foreign Keys Compuestas
        # En sistemas multi-tenant, buscar por (user_id) mata la performance si no está 
        # acotado por tenant. Debería ser (enterprise_id, user_id).
        # MySQL/MariaDB suele crear FK_index_1 solo para 'user_id'
        fk_columns = [c for c in cols if c.endswith('_id') and c not in ('id', 'enterprise_id')]
        
        for fk in fk_columns:
            if has_ent:
                # El índice compuesto ideal (enterprise_id, foreign_key_id)
                if not has_covering_index(table, ['enterprise_id', fk]):
                    # Si al menos es un FK importante (más de 1 posibilidad de join grande)
                    recommendations.append({
                        'table': table,
                        'sql': f"CREATE INDEX idx_{table}_ent_{fk} ON {table} (enterprise_id, {fk});",
                        'reason': f"Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por {fk} bajo el tenant actual."
                    })
            else:
                # Si no tiene enterprise_id, al menos indexar la FK sola si no lo está
                if not has_covering_index(table, [fk]):
                    recommendations.append({
                        'table': table,
                        'sql': f"CREATE INDEX idx_{table}_{fk} ON {table} ({fk});",
                        'reason': f"Falta índice FK en {fk} crítico para JOIN performance."
                    })

        # 2. Estrategia de Fechas de Reporte
        # Tablas core se filtran MUCHISIMO por fechas de emisión o creation.
        # Indexar solo fecha es lento en multi-tenant. Indexar (ent_id, date)
        if has_ent and table.startswith(core_prefixes):
            date_cols = [c for c in cols if cols[c]['type'] in ('date', 'datetime', 'timestamp') 
                         and c in ('fecha', 'fecha_emision', 'fecha_comprobante', 'created_at')]
            for dc in date_cols:
                if not has_covering_index(table, ['enterprise_id', dc]):
                    recommendations.append({
                        'table': table,
                        'sql': f"CREATE INDEX idx_{table}_ent_{dc} ON {table} (enterprise_id, {dc});",
                        'reason': f"Optimización de Búsqueda Histórica: Para reportes filtrados por {dc} en el tenant."
                    })

        # 3. Estrategia de Campos de Búsqueda de Texto (SKU, Nro Doc, Email)
        search_cols = ['codigo', 'sku', 'numero', 'nro_comprobante', 'email', 'cuit', 'numero_serie', 'barcode']
        for sc in search_cols:
            if sc in cols:
                idx_cols = ['enterprise_id', sc] if has_ent else [sc]
                if not has_covering_index(table, idx_cols):
                    # Solo indexar si es VARCHAR o numérico, no textos largos pero estos suelen ser varchar
                    prefix = "ent_" if has_ent else ""
                    recommendations.append({
                        'table': table,
                        'sql': f"CREATE INDEX idx_{table}_{prefix}{sc} ON {table} ({', '.join(idx_cols)});",
                        'reason': f"Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por {sc}"
                    })

        # 4. Estrategia de Estado (Filtros Selectivos booleanos/enums combinados)
        # NUNCA indexar (activo) solo, tiene bajísima cardinalidad (0 o 1).
        # PERO (enterprise_id, estado) donde estado es 'PENDIENTE', 'RECIBIDO', 'CANCELADO' es oro para listar.
        if has_ent and table.startswith(core_prefixes):
            if 'estado' in cols:
                if not has_covering_index(table, ['enterprise_id', 'estado']):
                    recommendations.append({
                        'table': table,
                        'sql': f"CREATE INDEX idx_{table}_ent_estado ON {table} (enterprise_id, estado);",
                        'reason': f"Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'"
                    })

    # Guardar Script de Tunning Consolidado
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'expert_tuning.sql')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("-- ==========================================\n")
        f.write("-- COLOSAL ERP EXPERT TUNING SCRIPT\n")
        f.write("-- Generado basado en patrones reales y \n")
        f.write("-- mejores prácticas Multi-Tenant / InnoDB\n")
        f.write("-- ==========================================\n\n")

        # Agrupar por tabla 
        recs_grouped = defaultdict(list)
        for r in recommendations:
            recs_grouped[r['table']].append(r)
            
        for tname in sorted(recs_grouped.keys()):
            f.write(f"\n-- 📦 TABLA: {tname}\n")
            for r in recs_grouped[tname]:
                f.write(f"-- Razón: {r['reason']}\n")
                f.write(f"{r['sql']}\n")
                
    print(f"✅ Master Tuning Guide generated ok: {filepath} with {len(recommendations)} rules.")

if __name__ == '__main__':
    advanced_index_tuning()
