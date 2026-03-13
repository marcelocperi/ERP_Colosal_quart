"""
Utilidad para inicializar datos maestros en nuevas empresas.

Este módulo se encarga de copiar los datos maestros desde la empresa global (enterprise_id=0)
a una nueva empresa cuando se crea.
"""

from database import get_db_cursor
import json
import os
import logging

logger = logging.getLogger(__name__)

# Tablas del Tax Engine que requieren tratamiento especial
# (referencian tax_impuestos por FK, que es global sin enterprise_id)
TAX_ENGINE_TABLES = {'tax_alicuotas', 'tax_reglas', 'tax_reglas_iibb'}


def load_init_config():
    """Carga la configuración de inicialización de empresas"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    config_path = os.path.join(project_root, '.agent', 'enterprise_init_config.json')

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


async def initialize_tax_engine(enterprise_id: int, existing_cursor=None) -> dict:
    """
    Copia las reglas fiscales del Tax Engine desde la plantilla global (enterprise_id=0)
    a la nueva empresa. El usuario podrá customizarlas luego desde Configuración > Impuestos.

    Las tablas copiadas son:
      - tax_alicuotas  : Alícuotas por impuesto y vigencia
      - tax_reglas     : Reglas por operación (COMPRAS/VENTAS/COBRANZAS/PAGOS)
      - tax_reglas_iibb: Reglas de IIBB por condición y jurisdicción

    Nota: tax_impuestos es global (sin enterprise_id) y NO se copia.
    """
    result = {'tables': {}, 'total': 0, 'errors': []}

    # Columnas a copiar por tabla (excluye 'id' que es auto-increment)
    TABLE_COLS = {
        'tax_alicuotas':   ['enterprise_id', 'impuesto_id', 'alicuota', 'base_calculo',
                            'vigencia_desde', 'vigencia_hasta', 'activo', 'observaciones'],
        'tax_reglas':      ['enterprise_id', 'operacion', 'tipo_responsable', 'condicion_iibb',
                            'impuesto_id', 'aplica', 'es_obligatorio', 'activo'],
        'tax_reglas_iibb': ['enterprise_id', 'condicion_iibb', 'jurisdiccion_codigo',
                            'jurisdiccion_nombre', 'impuesto_id', 'alicuota_override',
                            'usa_padron', 'regimen', 'limite_cm_pct', 'coef_minimo_cm', 'activo'],
    }

    from database import get_db_cursor
    with (await get_db_cursor(dictionary=True) if not existing_cursor else existing_cursor) as cursor:
        for table, cols in TABLE_COLS.items():
            try:
                # Verificar si ya tiene datos propios
                await cursor.execute(
                    f"SELECT COUNT(*) as cnt FROM {table} WHERE enterprise_id = %s",
                    (enterprise_id,)
                )
                if await cursor.fetchone()['cnt'] > 0:
                    logger.info(f"Tax Engine: {table} ya tiene datos para empresa {enterprise_id}, omitiendo.")
                    result['tables'][table] = {'status': 'skipped', 'reason': 'already_has_data'}
                    continue

                # Leer plantilla global
                await cursor.execute(
                    f"SELECT {', '.join(cols)} FROM {table} WHERE enterprise_id = 0"
                )
                rows = await cursor.fetchall()

                count = 0
                for row in rows:
                    vals = []
                    for col in cols:
                        if col == 'enterprise_id':
                            vals.append(enterprise_id)   # Asignar nueva empresa
                        else:
                            vals.append(row[col])

                    placeholders = ', '.join(['%s'] * len(cols))
                    col_names    = ', '.join([f'`{c}`' for c in cols])
                    await cursor.execute(
                        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})",
                        vals
                    )
                    count += 1

                result['tables'][table] = {'status': 'success', 'records_copied': count}
                result['total'] += count
                logger.info(f"Tax Engine: {table} → {count} reglas copiadas para empresa {enterprise_id}")

            except Exception as e:
                err = f"Error copiando {table}: {str(e)}"
                logger.error(err)
                result['tables'][table] = {'status': 'error', 'error': str(e)}
                result['errors'].append(err)

    return result


async def initialize_enterprise_master_data(enterprise_id: int, init_sod: bool = True, existing_cursor=None) -> dict:
    """
    Inicializa los datos maestros para una nueva empresa.
    
    Args:
        enterprise_id: ID de la empresa a inicializar
        
    Returns:
        dict: Resumen de la inicialización con contadores por tabla
    """
    if enterprise_id == 0:
        raise ValueError("No se puede inicializar la empresa global (ID=0)")
    
    config = load_init_config()
    results = {
        'enterprise_id': enterprise_id,
        'tables_copied': {},
        'errors': [],
        'total_records': 0
    }

    id_maps = {}  # Mapeo de {table_name: {old_id: new_id}}

    # El orden puede ser una lista directa o un dict con clave 'order'
    init_order_raw = config['initialization_order']
    if isinstance(init_order_raw, dict):
        init_order = init_order_raw.get('order', [])
    else:
        init_order = init_order_raw

    tax_engine_done = False  # Para procesar las 3 tablas del Tax Engine solo una vez

    from database import get_db_cursor
    with (await get_db_cursor(dictionary=True) if not existing_cursor else existing_cursor) as cursor:
        logger.info(f"Iniciando inicialización de datos maestros para empresa {enterprise_id}")

        # Procesar tablas en el orden especificado
        for table_name in init_order:

            # ── Tax Engine: tratamiento especial ──────────────────────────────
            if table_name in TAX_ENGINE_TABLES:
                if not tax_engine_done:
                    tax_result = await initialize_tax_engine(enterprise_id, existing_cursor=cursor)
                    for t, r in tax_result['tables'].items():
                        results['tables_copied'][t] = r
                    results['total_records'] += tax_result['total']
                    results['errors'].extend(tax_result['errors'])
                    tax_engine_done = True
                continue  # Las 3 tablas ya fueron procesadas en bloque
            # ─────────────────────────────────────────────────────────────────

            try:
                # Obtener columnas de la tabla
                await cursor.execute(f"DESC {table_name}")
                all_cols = [col['Field'] for col in await cursor.fetchall()]
                cols = [col for col in all_cols if col != 'id']  # Excluir ID auto-increment

                # Buscar configuración de la tabla
                table_config = None
                for category in config['master_data_tables'].values():
                    if isinstance(category, dict) and table_name in category:
                        table_config = category[table_name]
                        break

                if not table_config:
                    logger.warning(f"Tabla {table_name} no encontrada en configuración")
                    continue

                if not table_config.get('copy_from_global', False):
                    logger.info(f"Tabla {table_name} configurada para no copiar")
                    continue

                # Verificar si ya tiene datos
                await cursor.execute(
                    f"SELECT COUNT(*) as count FROM {table_name} WHERE enterprise_id = %s",
                    (enterprise_id,)
                )
                existing_count = await cursor.fetchone()['count']

                if existing_count > 0:
                    logger.warning(f"Tabla {table_name} ya tiene {existing_count} registros para empresa {enterprise_id}")
                    results['tables_copied'][table_name] = {
                        'status': 'skipped',
                        'reason': 'already_has_data',
                        'existing_records': existing_count
                    }
                    continue

                # Lógica especial para tablas jerárquicas o con FKs
                if table_name == 'cont_plan_cuentas':
                    copied_count, table_map = await _copy_hierarchical_table(cursor, table_name, enterprise_id, cols)
                    id_maps[table_name] = table_map
                elif table_name in ['erp_cuentas_fondos', 'fin_medios_pago']:
                    copied_count, table_map = await _copy_table_with_account_mapping(
                        cursor, table_name, enterprise_id, cols, id_maps.get('cont_plan_cuentas', {})
                    )
                    id_maps[table_name] = table_map
                else:
                    copied_count, table_map = await _copy_table_standard(cursor, table_name, enterprise_id, cols)
                    id_maps[table_name] = table_map

                results['tables_copied'][table_name] = {
                    'status': 'success',
                    'records_copied': copied_count,
                    'description': table_config.get('description', '')
                }
                results['total_records'] += copied_count
                logger.info(f"Tabla {table_name}: Copiados {copied_count} registros")

            except Exception as e:
                error_msg = f"Error copiando tabla {table_name}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['tables_copied'][table_name] = {
                    'status': 'error',
                    'error': str(e)
                }

        # Inicialización de SoD (Roles y Usuarios)
        if init_sod:
            try:
                try:
                    from services.sod_service import initialize_sod_structure
                except ImportError:
                    from sod_service import initialize_sod_structure

                await initialize_sod_structure(enterprise_id)
                results['sod_initialized'] = True
                logger.info("Estructura SoD inicializada correctamente")
            except Exception as e:
                err_msg = f"Error inicializando SoD: {str(e)}"
                logger.error(err_msg)
                results['sod_initialized'] = False
                results['errors'].append(err_msg)

        # Inicialización de Numeración (Puntos de Venta y Últimos Números)
        try:
            num_res = await initialize_enterprise_numeration(enterprise_id, existing_cursor=cursor)
            results['numeration_initialized'] = num_res['status'] == 'success'
            results['total_records'] += num_res.get('records_created', 0)
        except Exception as e:
            err_msg = f"Error inicializando numeración: {str(e)}"
            logger.error(err_msg)
            results['errors'].append(err_msg)

        logger.info(f"Inicialización completada. Total de registros copiados: {results['total_records']}")

    return results


async def get_master_data_summary(enterprise_id: int) -> dict:
    """
    Obtiene un resumen de los datos maestros de una empresa.
    
    Args:
        enterprise_id: ID de la empresa
        
    Returns:
        dict: Resumen con contadores por tabla
    """
    config = load_init_config()
    summary = {}
    
    async with get_db_cursor(dictionary=True) as cursor:
        for category_name, category_tables in config['master_data_tables'].items():
            summary[category_name] = {}
            
            for table_name, table_config in category_tables.items():
                try:
                    await cursor.execute(f"SELECT COUNT(*) as count FROM {table_name} WHERE enterprise_id = %s", (enterprise_id,))
                    count = await cursor.fetchone()['count']
                    
                    summary[category_name][table_name] = {
                        'count': count,
                        'description': table_config.get('description', ''),
                        'required': table_config.get('required', False)
                    }
                except Exception as e:
                    summary[category_name][table_name] = {
                        'count': 0,
                        'error': str(e)
                    }
    
    return summary

async def _copy_hierarchical_table(cursor, table_name, enterprise_id, cols):
    """
    Copia una tabla que tiene una relación jerárquica consigo misma (padre_id).
    Garantiza que el nuevo árbol apunte a los nuevos IDs.
    """
    # 1. Obtener todos los registros globales
    await cursor.execute(f"SELECT * FROM {table_name} WHERE enterprise_id = 0 ORDER BY id ASC")
    global_rows = await cursor.fetchall()
    
    id_map = {None: None}
    copied_count = 0
    
    # 2. Copiar manteniendo el mapeo de IDs
    # IMPORTANTE: El ORDER BY id ASC ayuda a que los padres se creen antes que los hijos en la mayoría de los casos
    # Si no, esta lógica debería ser recursiva o por niveles.
    for row in global_rows:
        old_id = row['id']
        
        # Preparar campos (excluyendo id y mapeando padre_id)
        current_cols = []
        current_vals = []
        placeholders = []
        
        for col in cols:
            current_cols.append(f"`{col}`")
            placeholders.append("%s")
            
            if col == 'enterprise_id':
                current_vals.append(enterprise_id)
            elif col == 'padre_id':
                # Mapear al nuevo ID si ya fue creado, si no, lo dejamos temporalmente en None
                current_vals.append(id_map.get(row['padre_id']))
            else:
                current_vals.append(row[col])
        
        sql = f"INSERT INTO {table_name} ({', '.join(current_cols)}) VALUES ({', '.join(placeholders)})"
        await cursor.execute(sql, current_vals)
        new_id = cursor.lastrowid
        id_map[old_id] = new_id
        copied_count += 1
        
    return copied_count, id_map

async def _copy_table_standard(cursor, table_name, enterprise_id, cols):
    """Copia estándar de una tabla plana"""
    await cursor.execute(f"SELECT * FROM {table_name} WHERE enterprise_id = 0")
    rows = await cursor.fetchall()
    
    id_map = {}
    copied_count = 0
    
    for row in rows:
        old_id = row['id']
        current_cols = []
        current_vals = []
        placeholders = []
        
        for col in cols:
            current_cols.append(f"`{col}`")
            placeholders.append("%s")
            if col == 'enterprise_id':
                current_vals.append(enterprise_id)
            else:
                current_vals.append(row[col])
        
        sql = f"INSERT INTO {table_name} ({', '.join(current_cols)}) VALUES ({', '.join(placeholders)})"
        await cursor.execute(sql, current_vals)
        new_id = cursor.lastrowid
        id_map[old_id] = new_id
        copied_count += 1
        
    return copied_count, id_map

async def _copy_table_with_account_mapping(cursor, table_name, enterprise_id, cols, account_map):
    """Copia de tablas que tienen cuenta_contable_id hacia el plan de cuentas"""
    await cursor.execute(f"SELECT * FROM {table_name} WHERE enterprise_id = 0")
    rows = await cursor.fetchall()
    
    id_map = {}
    copied_count = 0
    
    for row in rows:
        old_id = row['id']
        current_cols = []
        current_vals = []
        placeholders = []
        
        for col in cols:
            current_cols.append(f"`{col}`")
            placeholders.append("%s")
            if col == 'enterprise_id':
                current_vals.append(enterprise_id)
            elif col == 'cuenta_contable_id':
                # Mapear al nuevo ID de cuenta
                current_vals.append(account_map.get(row['cuenta_contable_id']))
            else:
                current_vals.append(row[col])
        
        sql = f"INSERT INTO {table_name} ({', '.join(current_cols)}) VALUES ({', '.join(placeholders)})"
        await cursor.execute(sql, current_vals)
        new_id = cursor.lastrowid
        id_map[old_id] = new_id
        copied_count += 1
        
    return copied_count, id_map


async def initialize_enterprise_numeration(enterprise_id: int, existing_cursor=None) -> dict:
    """
    Inicializa los parámetros de numeración para una nueva empresa.
    Crea una entrada en sys_enterprise_numeracion para cada tipo de comprobante activo.
    """
    result = {'status': 'success', 'records_created': 0}

    from database import get_db_cursor
    with (await get_db_cursor(dictionary=True) if not existing_cursor else existing_cursor) as cursor:
        try:
            # 1. Obtener todos los tipos de comprobante
            await cursor.execute("SELECT codigo FROM sys_tipos_comprobante")
            tipos = await cursor.fetchall()

            count = 0
            for t in tipos:
                await cursor.execute("""
                    INSERT IGNORE INTO sys_enterprise_numeracion 
                    (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, ultimo_numero)
                    VALUES (%s, 'COMPROBANTE', %s, 1, 0)
                """, (enterprise_id, t['codigo']))
                if cursor.rowcount > 0:
                    count += 1

            # 2. Otros tipos (opcionalmente)
            # Recibos, Ordenes de Pago, etc. podrían tener su propia lógica.
            # Por ahora inicializamos Recibos genéricos
            await cursor.execute("""
                INSERT IGNORE INTO sys_enterprise_numeracion 
                (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, ultimo_numero)
                VALUES (%s, 'RECIBO', 'RC', 1, 0)
            """, (enterprise_id,))
            if cursor.rowcount > 0:
                count += 1

            result['records_created'] = count
            logger.info(f"Numeración: {count} registros inicializados para empresa {enterprise_id}")

        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"Error inicializando numeración para empresa {enterprise_id}: {str(e)}")

    return result


async def sync_new_concept_to_all_enterprises(entidad_tipo: str, entidad_codigo: str) -> int:
    """
    Cuando se crea un nuevo concepto maestro (tipo de comprobante, impuesto, etc.),
    se asegura de que cada empresa tenga su entrada en la tabla de numeración.
    """
    from database import get_db_cursor
    count = 0
    async with get_db_cursor() as cursor:
        # Obtener todas las empresas
        await cursor.execute("SELECT id FROM sys_enterprises")
        enterprises = await cursor.fetchall()
        
        for (ent_id,) in enterprises:
            await cursor.execute("""
                INSERT IGNORE INTO sys_enterprise_numeracion 
                (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, ultimo_numero)
                VALUES (%s, %s, %s, 1, 0)
            """, (ent_id, entidad_tipo, entidad_codigo))
            if cursor.rowcount > 0:
                count += 1
                
    logger.info(f"Sincronizado nuevo concepto {entidad_tipo}:{entidad_codigo} para {count} empresas.")
    return count

if __name__ == "__main__":
    # Ejemplo de uso
    print("=== UTILIDAD DE INICIALIZACIÓN DE EMPRESAS ===\n")
    print("Esta utilidad se usa automáticamente al crear una nueva empresa.")
    print("\nEjemplo de uso:")
    print("  from enterprise_init import initialize_enterprise_master_data")
    print("  results = await initialize_enterprise_master_data(new_enterprise_id)")
    print("\nPara ver el resumen de una empresa:")
    print("  from enterprise_init import get_master_data_summary")
    print("  summary = await get_master_data_summary(enterprise_id)")
