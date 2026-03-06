from flask import Blueprint, render_template, request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required, permission_required
from database import get_db_cursor, atomic_transaction
import json
import csv
import io
import re
import threading
from datetime import datetime
from services import library_api_service
from services.book_service_factory import BookServiceFactory, NativeService
from services.georef_service import GeorefService
from services.validation_service import format_cuit
from enrich_books_api import process_until_finished
from core.safety_logic import get_incompatibility_alerts

stock_bp = Blueprint('stock', __name__, template_folder='templates', url_prefix='/stock')

from compras.fazon_routes import register_fazon_routes
register_fazon_routes(stock_bp)

# Registry to track active background tasks and avoid thread spamming/freezing
# (Moved to core.concurrency shared system)

@stock_bp.route('/dashboard')
@login_required
@permission_required('view_articulos')
def dashboard():
    """Tablero principal de Inventario y existencias
    NOTA: No actualizar este dashboard por ahora (pendiente para otra etapa)
    """
    try:
        ent_id = g.user['enterprise_id']
        search_query = request.args.get('q', '')
        deposito_id = request.args.get('deposito_id')
        
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Depósitos (Warehouse)
            cursor.execute("SELECT id, nombre FROM stk_depositos WHERE enterprise_id = %s AND activo = 1", (ent_id,))
            depositos = cursor.fetchall()
            
            # 2. Resumen de Existencias
            sql = """
                SELECT 
                    stk_articulos.nombre as articulo_nombre, stk_articulos.modelo as autor, stk_articulos.codigo as isbn, 
                    stk_depositos.nombre as deposito_nombre, 
                    stk_existencias.cantidad,
                    stk_articulos.id as articulo_id
                FROM stk_existencias
                JOIN stk_depositos ON stk_existencias.deposito_id = stk_depositos.id
                JOIN stk_articulos ON stk_existencias.articulo_id = stk_articulos.id
                WHERE stk_existencias.enterprise_id = %s
            """
            params = [ent_id]
            
            if search_query:
                sql += " AND (stk_articulos.nombre LIKE %s OR stk_articulos.codigo LIKE %s)"
                params.extend([f"%{search_query}%", f"%{search_query}%"])
            
            if deposito_id:
                sql += " AND stk_existencias.deposito_id = %s"
                params.append(deposito_id)
                
            sql += " ORDER BY stk_articulos.nombre ASC"
            
            cursor.execute(sql, tuple(params))
            items = cursor.fetchall()
            
            # Estadísticas rápidas
            cursor.execute("SELECT SUM(cantidad) as total FROM stk_existencias WHERE enterprise_id = %s", (ent_id,))
            total_stock = cursor.fetchone()['total'] or 0
    
            cursor.execute("""
                SELECT COUNT(*) as low_stock_count
                FROM (
                    SELECT stk_articulos.id
                    FROM stk_articulos
                    LEFT JOIN stk_existencias ON stk_articulos.id = stk_existencias.articulo_id AND stk_articulos.enterprise_id = stk_existencias.enterprise_id
                    WHERE stk_articulos.enterprise_id = %s
                    GROUP BY stk_articulos.id, stk_articulos.stock_minimo
                    HAVING (SELECT COALESCE(SUM(cantidad), 0) FROM stk_existencias WHERE articulo_id = stk_articulos.id AND enterprise_id = stk_articulos.enterprise_id) <= stk_articulos.stock_minimo 
                       AND stk_articulos.stock_minimo > 0
                ) as low_stock
            """, (ent_id,))
            low_stock_count = cursor.fetchone()['low_stock_count'] or 0
                
        return render_template('stock/dashboard.html', 
                               depositos=depositos, 
                               items=items, 
                               total_stock=total_stock,
                               low_stock_count=low_stock_count)
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f"Error al cargar el dashboard de stock: {str(e)}", "danger")
        return redirect('/')

@stock_bp.route('/movimientos/nuevo', methods=['GET', 'POST'])
@login_required
@atomic_transaction('stock', severity=7, impact_category='Operational')
def movimiento_crear():
    """Registro de movimientos de stock (Entrada, Salida, Transferencia)"""
    ent_id = g.user['enterprise_id']
    
    with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            motivo_id = request.form.get('motivo_id')
            dep_origen = request.form.get('dep_origen')
            dep_destino = request.form.get('dep_destino')
            tercero_id = request.form.get('tercero_id')
            articulo_id = request.form.get('articulo_id')
            cantidad = int(request.form.get('cantidad', 0))
            observaciones = request.form.get('observaciones')
            
            # SoD Control: El bypass solo es válido si el usuario tiene el permiso específico
            has_bypass_permission = 'safety_bypass' in g.permissions or 'all' in g.permissions
            
            if not articulo_id or cantidad <= 0:
                flash("Artículo y cantidad son requeridos.", "warning")
            else:
                try:
                    # Datos del motivo
                    cursor.execute("SELECT tipo, nombre FROM stk_motivos WHERE id = %s AND enterprise_id = %s", (motivo_id, ent_id))
                    motivo = cursor.fetchone()
                    if not motivo: raise Exception("Motivo inválido o no pertenece a la empresa.")
                    
                    tipo = motivo['tipo']
                    
                    # Validaciones de Depósitos
                    if tipo == 'SALIDA' and not dep_origen:
                        raise Exception("Se requiere Depósito de Origen para una Salida.")
                    if tipo == 'ENTRADA' and not dep_destino:
                        # Fallback simple
                        if dep_origen: dep_destino = dep_origen 
                        else: raise Exception("Se requiere Depósito de Destino para una Entrada.")
                    if tipo == 'TRANSFERENCIA' and (not dep_origen or not dep_destino):
                         raise Exception("Se requiere Origen y Destino para Transferencia.")

                    # --- SEGURIDAD INDUSTRIAL: Chequeo de Incompatibilidades ---
                    if dep_destino:
                         cursor.execute("""
                             SELECT stk_articulos_seguridad.*, stk_articulos.nombre as nombre_comun 
                             FROM stk_articulos_seguridad
                             JOIN stk_articulos ON stk_articulos_seguridad.articulo_id = stk_articulos.id
                             WHERE stk_articulos_seguridad.articulo_id = %s AND stk_articulos_seguridad.enterprise_id = %s
                         """, (articulo_id, ent_id))
                         incoming_s = cursor.fetchone()
                         
                         if incoming_s:
                             if incoming_s['pictogramas_json']:
                                 incoming_s['pictogramas_json'] = json.loads(incoming_s['pictogramas_json'])
                             
                             # Obtener lo que ya existe en el depósito de destino
                             cursor.execute("""
                                 SELECT stk_articulos_seguridad.*, stk_articulos.nombre as nombre_comun
                                 FROM stk_articulos_seguridad
                                 JOIN stk_existencias ON stk_articulos_seguridad.articulo_id = stk_existencias.articulo_id AND stk_articulos_seguridad.enterprise_id = stk_existencias.enterprise_id
                                 JOIN stk_articulos ON stk_articulos_seguridad.articulo_id = stk_articulos.id
                                 WHERE stk_existencias.deposito_id = %s AND stk_existencias.cantidad > 0 AND stk_articulos_seguridad.enterprise_id = %s
                             """, (dep_destino, ent_id))
                             existing_s = cursor.fetchall()
                             
                             for item in existing_s:
                                 if item['pictogramas_json']:
                                     item['pictogramas_json'] = json.loads(item['pictogramas_json'])
                             
                             safety_alerts = get_incompatibility_alerts(incoming_s, existing_s)

                             if safety_alerts:
                                 for alert in safety_alerts:
                                     flash(alert["message"], "danger" if alert["severity"] == "DANGER" else "warning")
                                     if alert["severity"] == "DANGER":
                                         if request.form.get("safety_bypass") == "on":
                                             if not ("safety_bypass" in g.permissions or "all" in g.permissions):
                                                 raise Exception(f"ACCESO DENEGADO (SoD): No tiene permiso 'safety_bypass' para omitir el bloqueo de: {alert['message']}")
                                         else:
                                             raise Exception(f"BLOQUEO DE SEGURIDAD INDUSTRIAL: {alert['message']}")
                    # Insertar Cabecera
                    cursor.execute("""
                        INSERT INTO stk_movimientos (enterprise_id, fecha, motivo_id, deposito_origen_id, deposito_destino_id, tercero_id, user_id, observaciones)
                        VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s)
                    """, (ent_id, motivo_id, dep_origen or None, dep_destino or None, tercero_id or None, g.user['id'], observaciones))
                    
                    cursor.execute("SELECT LAST_INSERT_ID() as id")
                    mov_id = cursor.fetchone()['id']
                    
                    # Insertar Detalle
                    cursor.execute("""
                        INSERT INTO stk_movimientos_detalle (enterprise_id, movimiento_id, articulo_id, cantidad, user_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (ent_id, mov_id, articulo_id, cantidad, g.user['id']))
                    
                    # ACTUALIZAR EXISTENCIAS
                    # Restar de Origen
                    if tipo in ['SALIDA', 'TRANSFERENCIA'] and dep_origen:
                        cursor.execute("""
                            UPDATE stk_existencias SET cantidad = cantidad - %s, last_updated = NOW(), user_id_update = %s
                            WHERE deposito_id = %s AND articulo_id = %s AND enterprise_id = %s
                        """, (cantidad, g.user['id'], dep_origen, articulo_id, ent_id))
                        if cursor.rowcount == 0:
                            cursor.execute("INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad) VALUES (%s, %s, %s, %s)", (ent_id, dep_origen, articulo_id, -cantidad))

                    # Sumar a Destino
                    if tipo in ['ENTRADA', 'TRANSFERENCIA'] and dep_destino:
                        cursor.execute("""
                            UPDATE stk_existencias SET cantidad = cantidad + %s, last_updated = NOW()
                            WHERE deposito_id = %s AND articulo_id = %s AND enterprise_id = %s
                        """, (cantidad, dep_destino, articulo_id, ent_id))
                        if cursor.rowcount == 0:
                            cursor.execute("INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad) VALUES (%s, %s, %s, %s)", (ent_id, dep_destino, articulo_id, cantidad))

                    flash(f"Movimiento '{motivo['nombre']}' registrado con éxito.", "success")
                    return redirect(url_for('stock.dashboard'))

                except Exception as e:
                    flash(f"Error: {e}", "danger")

        # Carga de combos para el formulario
        cursor.execute("SELECT id, nombre, tipo FROM stk_motivos WHERE enterprise_id = %s ORDER BY nombre", (ent_id,))
        motivos = cursor.fetchall()
        
        cursor.execute("SELECT id, nombre FROM stk_depositos WHERE enterprise_id = %s AND activo = 1", (ent_id,))
        depositos = cursor.fetchall()
        
        cursor.execute("SELECT id, nombre as titulo, codigo as isbn FROM stk_articulos WHERE enterprise_id = %s ORDER BY nombre", (ent_id,))
        articulos = cursor.fetchall()
        
        cursor.execute("SELECT id, nombre, es_cliente, es_proveedor FROM erp_terceros WHERE enterprise_id = %s AND activo = 1 ORDER BY nombre", (ent_id,))
        terceros = cursor.fetchall()
        
    return render_template('stock/movimiento_form.html', 
                           motivos=motivos, 
                           depositos=depositos, 
                           articulos=articulos,
                           terceros=terceros)
@stock_bp.route('/movimientos')
@login_required
@permission_required('view_movimientos')
def movimientos_historial():
    """Historial de movimientos de stock"""
    ent_id = g.user['enterprise_id']
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT 
                    stk_movimientos.id, stk_movimientos.fecha, stk_motivos.nombre as motivo, stk_motivos.tipo,
                    do.nombre as origen, dd.nombre as destino,
                    sys_users.username, stk_movimientos.observaciones, erp_terceros.nombre as tercero
                FROM stk_movimientos
                JOIN stk_motivos ON stk_movimientos.motivo_id = stk_motivos.id
                LEFT JOIN stk_depositos do ON stk_movimientos.deposito_origen_id = do.id
                LEFT JOIN stk_depositos dd ON stk_movimientos.deposito_destino_id = dd.id
                LEFT JOIN sys_users ON stk_movimientos.user_id = sys_users.id
                LEFT JOIN erp_terceros ON stk_movimientos.tercero_id = erp_terceros.id
                WHERE stk_movimientos.enterprise_id = %s
                ORDER BY stk_movimientos.fecha DESC
                LIMIT 100
            """, (ent_id,))
            movimientos = cursor.fetchall()
            
        return render_template('stock/movimientos_historial.html', movimientos=movimientos)
    except Exception as e:
        flash(f"Error cargando historial: {e}", "danger")
        return redirect(url_for('stock.dashboard'))

@stock_bp.route('/movimientos/detalle/<int:id>')
@login_required
def movimiento_detalle(id):
    """Ver detalle de un movimiento específico"""
    ent_id = g.user['enterprise_id']
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Cabecera
            cursor.execute("""
                SELECT 
                    stk_movimientos.id, stk_movimientos.fecha, stk_motivos.nombre as motivo, stk_motivos.tipo,
                    do.nombre as origen, dd.nombre as destino,
                    sys_users.username, stk_movimientos.observaciones, erp_terceros.nombre as tercero
                FROM stk_movimientos
                JOIN stk_motivos ON stk_movimientos.motivo_id = stk_motivos.id
                LEFT JOIN stk_depositos do ON stk_movimientos.deposito_origen_id = do.id
                LEFT JOIN stk_depositos dd ON stk_movimientos.deposito_destino_id = dd.id
                LEFT JOIN sys_users ON stk_movimientos.user_id = sys_users.id
                LEFT JOIN erp_terceros ON stk_movimientos.tercero_id = erp_terceros.id
                WHERE stk_movimientos.id = %s AND stk_movimientos.enterprise_id = %s
            """, (id, ent_id))
            mov = cursor.fetchone()
            
            if not mov:
                flash("Movimiento no encontrado.", "danger")
                return redirect(url_for('stock.movimientos_historial'))
                
            # Detalle de items
            cursor.execute("""
                SELECT stk_articulos.nombre as articulo, stk_articulos.codigo as isbn, stk_movimientos_detalle.cantidad
                FROM stk_movimientos_detalle
                JOIN stk_articulos ON stk_movimientos_detalle.articulo_id = stk_articulos.id
                WHERE stk_movimientos_detalle.movimiento_id = %s
            """, (id,))
            detalles = cursor.fetchall()
            
        return render_template('stock/movimiento_detalle.html', mov=mov, detalles=detalles)
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.movimientos_historial'))

@stock_bp.route('/historial/<int:id>')
@login_required
def articulo_historial(id):
    """Historial de movimientos para un artículo específico"""
    ent_id = g.user['enterprise_id']
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Info del artículo
            cursor.execute("SELECT nombre, codigo as isbn FROM stk_articulos WHERE id = %s AND enterprise_id = %s", (id, ent_id))
            articulo = cursor.fetchone()
            
            if not articulo:
                flash("Artículo no encontrado.", "danger")
                return redirect(url_for('stock.articulos'))
                
            # Movimientos vinculados
            cursor.execute("""
                SELECT 
                    stk_movimientos.id as movimiento_id, stk_movimientos.fecha, stk_motivos.nombre as motivo, stk_motivos.tipo,
                    do.nombre as origen, dd.nombre as destino,
                    sys_users.username, stk_movimientos.observaciones, erp_terceros.nombre as tercero,
                    stk_movimientos_detalle.cantidad
                FROM stk_movimientos_detalle
                JOIN stk_movimientos ON stk_movimientos_detalle.movimiento_id = stk_movimientos.id
                JOIN stk_motivos ON stk_movimientos.motivo_id = stk_motivos.id
                LEFT JOIN stk_depositos do ON stk_movimientos.deposito_origen_id = do.id
                LEFT JOIN stk_depositos dd ON stk_movimientos.deposito_destino_id = dd.id
                LEFT JOIN sys_users ON stk_movimientos.user_id = sys_users.id
                LEFT JOIN erp_terceros ON stk_movimientos.tercero_id = erp_terceros.id
                WHERE stk_movimientos_detalle.articulo_id = %s AND stk_movimientos.enterprise_id = %s
                ORDER BY stk_movimientos.fecha DESC
                LIMIT 200
            """, (id, ent_id))
            historial = cursor.fetchall()
            
        return render_template('stock/articulo_historial.html', 
                             articulo=articulo, 
                             historial=historial)
    except Exception as e:
        flash(f"Error cargando historial del artículo: {e}", "danger")
        return redirect(url_for('stock.articulos'))

@stock_bp.route('/articulos')
@login_required
def articulos():
    """Maestro de Artículos desde la perspectiva de Stock (con funcionalidad de Biblioteca)"""
    ent_id = g.user['enterprise_id']
    # Filtros avanzados
    q = request.args.get('q', '')
    filter_autor = request.args.get('autor', '')
    filter_editorial = request.args.get('editorial', '')
    filter_genero = request.args.get('genero', '')
    filter_lengua = request.args.get('lengua', '')
    filter_origen = request.args.get('origen', '')
    filter_con_portada = request.args.get('con_portada') == '1'
    filter_con_detalle = request.args.get('con_detalle') == '1'
    filter_con_ebook = request.args.get('con_ebook') == '1'
    
    # Paginación
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Query base para contar total de registros
            count_sql = """
                SELECT COUNT(*) as total
                FROM stk_articulos
                WHERE stk_articulos.enterprise_id = %s OR stk_articulos.enterprise_id = 0
            """
            count_params = [ent_id]
            
            # Query optimizada con información de préstamos (Uso de Joins para velocidad)
            sql = """
                SELECT stk_articulos.*, 
                       JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.genero')) as genero,
                       JSON_EXTRACT(stk_articulos.metadata_json, '$.paginas') as paginas, 
                       JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.fecha_pub')) as fecha_publicacion,
                       stk_articulos.stock_minimo, 
                       JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.lengua')) as lengua,
                       JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.origen')) as origen,
                       stk_articulos.codigo as isbn, stk_articulos.modelo as autor,
                       stk_articulos.marca as editorial,
                       JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.cover_url')) as cover_url,
                       COALESCE(NULLIF(stk_articulos.descripcion, ''), JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.descripcion'))) as descripcion,
                       JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.temas')) as temas,
                       JSON_EXTRACT(stk_articulos.metadata_json, '$.archivo_local') = 'true' as archivo_local,
                       IFNULL(e.total_cantidad, 0) as stock_total,
                       IFNULL(p.total_prestados, 0) as prestados,
                       (IFNULL(e.total_cantidad, 0) - IFNULL(p.total_prestados, 0)) as disponibles
                FROM stk_articulos
                LEFT JOIN (
                    SELECT articulo_id, SUM(cantidad) as total_cantidad
                    FROM stk_existencias
                    WHERE enterprise_id = %s
                    GROUP BY articulo_id
                ) e ON stk_articulos.id = e.articulo_id
                LEFT JOIN (
                    SELECT libro_id, COUNT(*) as total_prestados
                    FROM prestamos
                    WHERE enterprise_id = %s AND fecha_devolucion_real IS NULL
                    GROUP BY libro_id
                ) p ON stk_articulos.id = p.libro_id
                WHERE stk_articulos.enterprise_id = %s OR stk_articulos.enterprise_id = 0
            """
            params = [ent_id, ent_id, ent_id]
            
            # Búsqueda general
            if q:
                filter_clause = " AND (stk_articulos.nombre LIKE %s OR stk_articulos.codigo LIKE %s OR stk_articulos.modelo LIKE %s OR stk_articulos.marca LIKE %s OR stk_articulos.descripcion LIKE %s)"
                sql += filter_clause
                count_sql += filter_clause
                q_param = f"%{q}%"
                params.extend([q_param, q_param, q_param, q_param, q_param])
                count_params.extend([q_param, q_param, q_param, q_param, q_param])
            
            # Filtros específicos (Ahora usando JSON_EXTRACT para los campos movidos)
            if filter_autor:
                sql += " AND stk_articulos.modelo = %s"
                count_sql += " AND stk_articulos.modelo = %s"
                params.append(filter_autor)
                count_params.append(filter_autor)
            if filter_editorial:
                sql += " AND stk_articulos.marca = %s"
                count_sql += " AND stk_articulos.marca = %s"
                params.append(filter_editorial)
                count_params.append(filter_editorial)
            if filter_genero:
                sql += " AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.genero')) = %s"
                count_sql += " AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.genero')) = %s"
                params.append(filter_genero)
                count_params.append(filter_genero)
            if filter_lengua:
                sql += " AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.lengua')) = %s"
                count_sql += " AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.lengua')) = %s"
                params.append(filter_lengua)
                count_params.append(filter_lengua)
            if filter_origen:
                sql += " AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.origen')) = %s"
                count_sql += " AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.origen')) = %s"
                params.append(filter_origen)
                count_params.append(filter_origen)
            
            # Filtros booleanos técnicos
            if filter_con_portada:
                # Busca que exista una URL de portada válida en el metadato
                clause = " AND (JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.cover_url')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.cover_url')) NOT IN ('null', '', '[]', '{}'))"
                sql += clause
                count_sql += clause
            if filter_con_detalle:
                # Busca que tenga descripción ya sea en la columna nativa o en el metadato
                clause = """ AND (
                    (stk_articulos.descripcion IS NOT NULL AND stk_articulos.descripcion != '') OR 
                    (JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.descripcion')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.descripcion')) NOT IN ('null', '', '[]', '{}'))
                )"""
                sql += clause
                count_sql += clause
            
            if filter_con_ebook:
                clause = " AND JSON_UNQUOTE(JSON_EXTRACT(stk_articulos.metadata_json, '$.archivo_local')) = 'true'"
                sql += clause
                count_sql += clause
            
            # Obtener total de registros
            cursor.execute(count_sql, tuple(count_params))
            total_items = cursor.fetchone()['total']
            total_pages = (total_items + per_page - 1) // per_page  # Ceiling division
            
            # Agregar ORDER BY, LIMIT y OFFSET
            sql += " ORDER BY stk_articulos.nombre ASC LIMIT %s OFFSET %s"
            params.extend([per_page, offset])
            
            cursor.execute(sql, tuple(params))
            articulos_data = cursor.fetchall()

            # Limpiar descripciones que vienen como objetos JSON (ej: Open Library)
            for a in articulos_data:
                if a.get('descripcion') and a['descripcion'].startswith('{'):
                    try:
                        d_obj = json.loads(a['descripcion'])
                        if isinstance(d_obj, dict) and 'value' in d_obj:
                            a['descripcion'] = d_obj['value']
                    except: pass

            # Obtener Tipos para el modal
            # Select specific columns to ensure clean JSON serialization
            cursor.execute("SELECT id, nombre, custom_fields_schema, naturaleza FROM stk_tipos_articulo WHERE enterprise_id = %s OR enterprise_id = 0 ORDER BY nombre", (ent_id,))
            tipos = cursor.fetchall()
            
            # Serialize for JS
            tipos_list = []
            for t in tipos:
                # Convert schemas to object if string, for clean re-dumping? No, keep as is or parse?
                # Actually, if custom_fields_schema comes as string from DB, we can pass it as is, or parse it.
                # Let's clean it up.
                d = {'id': t['id'], 'nombre': t['nombre'], 'naturaleza': t.get('naturaleza', 'PRODUCTO')}
                schema = t.get('custom_fields_schema')
                if schema:
                    if isinstance(schema, str):
                        try:
                            d['custom_fields_schema'] = json.loads(schema)
                        except:
                            d['custom_fields_schema'] = []
                    else:
                        d['custom_fields_schema'] = schema # Already dict/list if driver handles JSON
                else:
                    d['custom_fields_schema'] = []
                tipos_list.append(d)
                
            tipos_json = json.dumps(tipos_list)
            
            # Obtener usuarios para préstamos (SOCIOS/CLIENTES)
            cursor.execute("SELECT id, nombre, apellido, email FROM usuarios WHERE enterprise_id = %s ORDER BY apellido LIMIT 100", (ent_id,))
            usuarios = cursor.fetchall()
            
            # Obtener valores distintos para los filtros
            cursor.execute("SELECT DISTINCT modelo FROM stk_articulos WHERE enterprise_id = %s AND modelo IS NOT NULL ORDER BY modelo", (ent_id,))
            autores = [r['modelo'] for r in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT marca FROM stk_articulos WHERE enterprise_id = %s AND marca IS NOT NULL ORDER BY marca", (ent_id,))
            editoriales = [r['marca'] for r in cursor.fetchall()]
            
            # Obtener valores distintos para los filtros (Optimizado con columnas virtuales)
            cursor.execute("SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.genero')) as genero FROM stk_articulos WHERE enterprise_id = %s ORDER BY genero", (ent_id,))
            generos = [r['genero'] for r in cursor.fetchall() if r['genero']]
            
            cursor.execute("SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.lengua')) as lengua FROM stk_articulos WHERE enterprise_id = %s ORDER BY lengua", (ent_id,))
            lenguas = [r['lengua'] for r in cursor.fetchall() if r['lengua']]
            
            cursor.execute("SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.origen')) as origen FROM stk_articulos WHERE enterprise_id = %s ORDER BY origen", (ent_id,))
            origenes = [r['origen'] for r in cursor.fetchall() if r['origen']]
            
            # Count pending enrichment for the UI button badge (Based on missing cover_url or descripcion in metadata)
            cursor.execute("""
                SELECT COUNT(*) as pending 
                FROM stk_articulos 
                WHERE enterprise_id = %s 
                AND (JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.cover_url')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.cover_url')) = '')
            """, (ent_id,))
            pending_count = cursor.fetchone()['pending'] or 0
            
        return render_template('stock/articulos.html', 
                             articulos=articulos_data, 
                             pending_count=pending_count,
                             autores=autores,
                             editoriales=editoriales,
                             generos=generos,
                             lenguas=lenguas,
                             origenes=origenes,
                             usuarios=usuarios,  # Para modal de préstamo
                             # Valores actuales de filtros para mantener estado
                             current_filters={
                                 'q': q,
                                 'autor': filter_autor,
                                 'editorial': filter_editorial,
                                 'genero': filter_genero,
                                 'lengua': filter_lengua,
                                 'origen': filter_origen,
                                 'con_portada': filter_con_portada,
                                 'con_detalle': filter_con_detalle
                             },
                             # Paginación
                             page=page,
                             total_pages=total_pages,
                             total_items=total_items,
                             per_page=per_page,
                             tipos=tipos,
                             tipos_json=tipos_json) # Pass full types config for JS
    except Exception as e:
        flash(f"Error cargando artículos: {e}", "danger")
        return redirect(url_for('stock.dashboard'))

@stock_bp.route('/articulos/guardar', methods=['POST'])
@login_required
@permission_required('books_add')
@atomic_transaction('stock', severity=5, impact_category='Technical')
def articulo_guardar():
    """Crear o actualizar un artículo (Producto, Servicio, Abono)"""
    ent_id = g.user['enterprise_id']
    id_articulo = request.form.get('id')
    
    try:
        nombre = request.form.get('nombre', '')
        autor = request.form.get('autor', '')
        isbn = request.form.get('isbn', '')
        genero = request.form.get('genero')
        precio = request.form.get('precio')
        editorial = request.form.get('editorial')
        tipo_id = request.form.get('tipo_articulo_id', 1) 
        fecha_pub = request.form.get('fecha_publicacion')
        desc_nueva = request.form.get('descripcion')
        
        # Financial Fields
        costo = request.form.get('costo', 0)
        costo_reposicion = request.form.get('costo_reposicion', 0)
        metodo_costeo = request.form.get('metodo_costeo', 'CPP')
        punto_pedido = request.form.get('punto_pedido', 0)
        stock_minimo = request.form.get('stock_minimo', 0)
        cant_min_pedido = request.form.get('cant_min_pedido', 1)
        
        try:
            punto_pedido = int(punto_pedido) if punto_pedido else 0
            stock_minimo = int(stock_minimo) if stock_minimo else 0
            cant_min_pedido = int(cant_min_pedido) if cant_min_pedido else 1
            if cant_min_pedido < 1: cant_min_pedido = 1
        except:
            pass
            
        # New Fields
        naturaleza = request.form.get('naturaleza', 'PRODUCTO')
        requiere_serie = request.form.get('requiere_serie') == 'on'
        patron_serie = request.form.get('patron_serie')
        genera_serie = request.form.get('genera_serie_automatica') == '1'
        sku_origen = request.form.get('sku_origen', 'PROPIO')
        
        # Service Config
        config_servicio = None
        es_recurrente = 0
        
        if naturaleza in ('SERVICIO', 'ABONO'):
            recurrencia = request.form.get('service_recurrencia', 'EVENTUAL')
            duracion = request.form.get('service_duracion', 0)
            es_recurrente_chk = request.form.get('es_recurrente') == 'on'
            
            config_servicio = json.dumps({
                'recurrencia': recurrencia,
                'duracion': int(duracion) if duracion else 0,
                'es_recurrente': es_recurrente_chk
            })
            es_recurrente = 1 if es_recurrente_chk else 0

        with get_db_cursor() as cursor:
            # metadata preparation
            if id_articulo:
                # Update existing
                cursor.execute("SELECT metadata_json FROM stk_articulos WHERE id=%s AND enterprise_id=%s", (id_articulo, ent_id))
                row = cursor.fetchone()
                metadata = json.loads(row[0]) if row and row[0] else {}
            else:
                # New
                metadata = {}
            
            # Map legacy form fields to metadata for backward compatibility (and update physical columns)
            if genero: metadata['genero'] = genero
            if fecha_pub: metadata['fecha_pub'] = fecha_pub
            if desc_nueva is not None: metadata['descripcion'] = desc_nueva

            # Process Custom Fields (Campos Propietarios)
            cust_json = request.form.get('custom_fields_json')
            if cust_json:
                try:
                    cust_data = json.loads(cust_json)
                    if isinstance(cust_data, dict):
                        # Merge custom fields into metadata
                        metadata.update(cust_data)
                        
                        # Extra logic: Map certain metadata keys back to physical columns if they exist in schema
                        if 'modelo' in cust_data: autor = cust_data['modelo']
                        if 'marca' in cust_data: editorial = cust_data['marca']
                        if 'codigo' in cust_data: isbn = cust_data['codigo']
                        if 'v_genero' in cust_data: metadata['genero'] = cust_data['v_genero']
                        if 'v_fecha_pub' in cust_data: metadata['fecha_pub'] = cust_data['v_fecha_pub']

                except Exception as e:
                    print(f"Error parsing custom fields: {e}")

            if id_articulo: # UPDATE
                cursor.execute("""
                    UPDATE stk_articulos SET 
                        nombre=%s, modelo=%s, codigo=%s, marca=%s, precio_venta=%s, metadata_json=%s, tipo_articulo_id=%s,
                        naturaleza=%s, es_recurrente=%s, config_servicio_json=%s,
                        requiere_serie=%s, patron_serie=%s, genera_serie_automatica=%s,
                        costo=%s, costo_reposicion=%s, fecha_costo_reposicion=IF(%s != costo_reposicion, %s, fecha_costo_reposicion),
                        metodo_costeo=%s, sku_origen=%s, cant_min_pedido=%s,
                        punto_pedido=%s, stock_minimo=%s
                    WHERE id=%s AND enterprise_id=%s
                """, (
                    nombre, autor, isbn, editorial, precio or 0, json.dumps(metadata), tipo_id,
                    naturaleza, es_recurrente, config_servicio,
                    1 if requiere_serie else 0, patron_serie, 1 if genera_serie else 0,
                    costo, costo_reposicion, costo_reposicion, datetime.date.today(),
                    metodo_costeo, sku_origen, cant_min_pedido,
                    punto_pedido, stock_minimo,
                    id_articulo, ent_id
                ))
                
                # --- Seguridad Industrial (Fase 5.1) ---
                if request.form.get('has_safety_data') == 'on':
                    if 'industrial_safety' in g.permissions or 'all' in g.permissions:
                        pictos = request.form.getlist('ghs_pictos') # Lista de códigos GHSXX
                        cursor.execute("""
                            INSERT INTO stk_articulos_seguridad 
                            (articulo_id, enterprise_id, numero_un, clase_riesgo, nombre_tecnico, 
                             instrucciones_estibaje, frases_h, frases_p, pictogramas_json, forma_estibaje, incompatibilidades)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            numero_un=VALUES(numero_un), clase_riesgo=VALUES(clase_riesgo), nombre_tecnico=VALUES(nombre_tecnico),
                            instrucciones_estibaje=VALUES(instrucciones_estibaje), frases_h=VALUES(frases_h), frases_p=VALUES(frases_p),
                            pictogramas_json=VALUES(pictogramas_json), forma_estibaje=VALUES(forma_estibaje), incompatibilidades=VALUES(incompatibilidades)
                        """, (
                            id_articulo, ent_id, request.form.get('sec_un'), request.form.get('sec_clase'),
                            request.form.get('sec_nombre_tecnico'), request.form.get('sec_instrucciones'),
                            request.form.get('sec_frases_h'), request.form.get('sec_frases_p'),
                            json.dumps(pictos), request.form.get('sec_estibaje'), request.form.get('sec_incompat')
                        ))
                    else:
                        flash("No tiene permisos para modificar datos de seguridad industrial.", "warning")
                
                flash(f"{naturaleza.capitalize()} actualizado", "success")

            else: # CREATE
                cursor.execute("""
                    INSERT INTO stk_articulos (
                        enterprise_id, nombre, modelo, codigo, marca, precio_venta, 
                        metadata_json, tipo_articulo, tipo_articulo_id,
                        naturaleza, es_recurrente, config_servicio_json, 
                        requiere_serie, patron_serie, genera_serie_automatica,
                        costo, costo_reposicion, fecha_costo_reposicion, metodo_costeo,
                        sku_origen, cant_min_pedido, punto_pedido, stock_minimo
                    ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'mercaderia', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    ent_id, nombre, autor, isbn, editorial, precio or 0, 
                    json.dumps(metadata), tipo_id,
                    naturaleza, es_recurrente, config_servicio,
                    1 if requiere_serie else 0, patron_serie, 1 if genera_serie else 0,
                    costo, costo_reposicion, datetime.date.today() if costo_reposicion else None,
                    metodo_costeo, sku_origen, cant_min_pedido, punto_pedido, stock_minimo
                ))
                new_id = cursor.lastrowid
                
                # --- Seguridad Industrial (Fase 5.1) ---
                if request.form.get('has_safety_data') == 'on':
                    if 'industrial_safety' in g.permissions or 'all' in g.permissions:
                        pictos = request.form.getlist('ghs_pictos')
                        cursor.execute("""
                            INSERT INTO stk_articulos_seguridad 
                            (articulo_id, enterprise_id, numero_un, clase_riesgo, nombre_tecnico, 
                             instrucciones_estibaje, frases_h, frases_p, pictogramas_json, forma_estibaje, incompatibilidades)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            new_id, ent_id, request.form.get('sec_un'), request.form.get('sec_clase'),
                            request.form.get('sec_nombre_tecnico'), request.form.get('sec_instrucciones'),
                            request.form.get('sec_frases_h'), request.form.get('sec_frases_p'),
                            json.dumps(pictos), request.form.get('sec_estibaje'), request.form.get('sec_incompat')
                        ))
                    else:
                        flash("Se creó el artículo pero se omitieron datos de seguridad por falta de permisos.", "warning")
                
                flash(f"{naturaleza.capitalize()} agregado exitosamente", "success")

    except Exception as e:
        flash(f"Error al guardar artículo: {e}", "danger")
    
    return redirect(url_for('stock.articulos'))

@stock_bp.route('/api/articulos/<int:articulo_id>/seguridad', methods=['GET'])
@login_required
def api_get_safety_data(articulo_id):
    """Obtiene los datos de seguridad industrial de un artículo"""
    # Control de acceso a datos sensibles de seguridad
    if 'view_articulos' not in g.permissions and 'industrial_safety' not in g.permissions and 'all' not in g.permissions:
        return jsonify({"success": False, "message": "Acceso insuficiente para datos de seguridad"}), 403
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM stk_articulos_seguridad WHERE articulo_id = %s AND enterprise_id = %s", (articulo_id, ent_id))
        data = cursor.fetchone()
    
    if data and data['pictogramas_json']:
        try:
            data['pictogramas_json'] = json.loads(data['pictogramas_json'])
        except:
            data['pictogramas_json'] = []
            
    return jsonify({'success': True, 'data': data or {}})

@stock_bp.route('/articulos/importar', methods=['GET', 'POST'])
@login_required
@permission_required('books_import')
def articulos_importar():
    if request.method == 'POST':
        try:
            idioma = request.form.get('idioma')
            cantidad = int(request.form.get('cantidad', 10))
            source = request.form.get('source', 'openlibrary')
            libros_externos = library_api_service.search_books_by_language(idioma, cantidad, source=source)
            altas, duplicados = 0, 0
            libros_agregados = []
            
            with get_db_cursor() as cursor:
                for lib in libros_externos:
                    isbn = lib['isbn']
                    cursor.execute("SELECT id FROM stk_articulos WHERE codigo = %s AND enterprise_id = %s", (isbn, g.user['enterprise_id']))
                    if cursor.fetchone():
                        duplicados += 1
                        continue
                    
                    lengua = idioma[:3].lower() if idioma else 'UND'
                    origen = "Local" if lengua == "spa" else "Importado"
                    metadata = {
                        "genero": "Desconocido",
                        "fecha_publicacion": str(lib['year']),
                        "paginas": lib['pages']
                    }
                    
                    cursor.execute(
                        "INSERT INTO stk_articulos (enterprise_id, nombre, modelo, codigo, marca, precio_venta, lengua, origen, api_checked, metadata_json, tipo_articulo, tipo_articulo_id) VALUES (%s, %s, %s, %s, %s, 0, %s, %s, 1, %s, 'mercaderia', 1)",
                        (g.user['enterprise_id'], lib['title'], lib['author'], isbn, lib['publisher'], lengua, origen, json.dumps(metadata))
                    )
                    altas += 1
                    libros_agregados.append(lib)
            flash(f"Importación: {altas} altas, {duplicados} duplicados.", "success")
            return render_template('stock/importar_articulos.html', resultados={'altas': altas, 'duplicados': duplicados}, libros_agregados=libros_agregados)
        except Exception as e: flash(str(e), "danger")
    return render_template('stock/importar_articulos.html')

@stock_bp.route('/articulos/upload', methods=['POST'])
@login_required
@permission_required('books_import')
def articulos_upload_csv():
    ent_id = g.user['enterprise_id']
    if 'file' not in request.files: return redirect(url_for('stock.articulos'))
    file = request.files['file']
    if file and file.filename.endswith('.csv'):
        try:
            stream = file.read().decode("utf-8").splitlines()
            lector = csv.DictReader(stream)
            count = 0
            with get_db_cursor() as cursor:
                for fila in lector:
                    isbn = fila.get('isbn', '').strip()
                    if not isbn: continue
                    cursor.execute("SELECT id FROM stk_articulos WHERE codigo = %s AND enterprise_id = %s", (isbn, ent_id))
                    if cursor.fetchone(): continue
                    cursor.execute("INSERT INTO stk_articulos (enterprise_id, nombre, modelo, codigo, precio_venta, api_checked, tipo_articulo) VALUES (%s,%s,%s,%s,%s, 0, 'mercaderia')",
                                   (ent_id, fila.get('nombre'), fila.get('autor'), isbn, fila.get('precio', 0)))
                    count += 1
            flash(f"CSV procesado: {count} artículos importados.", "success")
        except Exception as e: flash(str(e), "danger")
    return redirect(url_for('stock.articulos'))

@stock_bp.route('/articulos/enrich', methods=['POST'])
@login_required
def enrich_bulk():
    """Ejecuta el enriquecimiento masivo en segundo plano"""
    with open("route_hit.log", "a") as f:
        f.write(f"[{datetime.now()}] enrich_bulk HIT. deep={request.args.get('deep')}, strategy={request.form.get('strategy')}\n")
    
    import threading
    import os
    
    deep = request.args.get('deep') == '1'
    strategy = request.form.get('strategy', 'conservative')
    ent_id = g.user['enterprise_id']
    from core.concurrency import get_active_tasks, clear_stop_signal
    
    # Check if a task is already running
    if f"enrich_{ent_id}" in get_active_tasks():
        flash("Ya hay un proceso de enriquecimiento en curso. Por favor espere.", "warning")
        return redirect(url_for('stock.articulos'))

    # Asegurar señal limpia antes de disparar
    clear_stop_signal(f"enrich_{ent_id}")
    
    # ENRIQUECIMIENTO REACTIVADO
    import subprocess
    cmd = [sys.executable, "enrich_books_api.py", "--enterprise", str(ent_id), "--strategy", strategy]
    if deep: cmd.append("--deep")
    
    creationflags = 0x08000000 if os.name == 'nt' else 0
    subprocess.Popen(cmd, cwd=os.getcwd(), creationflags=creationflags)
    
    flash("Enriquecimiento iniciado en segundo plano. Puede monitorear el progreso en la tabla de artículos.", "success")
    return redirect(url_for('stock.articulos'))

@stock_bp.route('/depositos')
@login_required
def depositos_lista():
    """Listado de Depósitos/Almacenes"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM stk_depositos WHERE enterprise_id = %s", (ent_id,))
            depositos = cursor.fetchall()
        return render_template('stock/depositos.html', depositos=depositos)
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.dashboard'))

@stock_bp.route('/api/pending-count')
@login_required
def api_pending_count():
    """Retorna contadores simplificados mientras el enriquecimiento está en pausa"""
    return jsonify({
        "pending": 0, 
        "processed": 0,
        "deep_pending": 0,
        "status": 'Inactivo',
        "is_active": False,
        "stats": {}
    })

@stock_bp.route('/articulos/enrich/last-report')
@login_required
def enrich_last_report():
    import os
    import glob
    from flask import send_file

    files = glob.glob(os.path.join(os.getcwd(), "libros_enriquecidos_*.xlsx"))
    if not files:
        flash("No se encontró ningún reporte generado.", "info")
        return redirect(url_for('stock.articulos'))

    # Get the latest file
    latest_file = max(files, key=os.path.getmtime)
    
    try:
        return send_file(latest_file, as_attachment=True)
    except Exception as e:
        flash(f"Error al descargar reporte: {e}", "danger")
        return redirect(url_for('stock.articulos'))

@stock_bp.route('/depositos/nuevo', methods=['GET', 'POST'])
@login_required
def deposito_nuevo():
    """Crear nuevo depósito"""
    ent_id = g.user['enterprise_id']
    if request.method == 'POST':
        nombre = request.form['nombre']
        tipo = request.form.get('tipo', 'INTERNO')
        tercero_id = request.form.get('tercero_id')
        if not tercero_id: tercero_id = None
        
        calle = request.form.get('calle', '')
        numero = request.form.get('numero', '')
        localidad = request.form.get('localidad', '')
        provincia = request.form.get('provincia', '')
        cod_postal = request.form.get('cod_postal', '')
        direccion = f"{calle} {numero} - {localidad}, {provincia}"
        es_principal = 1 if 'es_principal' in request.form else 0
        
        try:
            with get_db_cursor(dictionary=True) as cursor:
                if es_principal:
                    cursor.execute("UPDATE stk_depositos SET es_principal = 0 WHERE enterprise_id = %s", (ent_id,))
                
                cursor.execute("""
                    INSERT INTO stk_depositos (enterprise_id, nombre, tipo, tercero_id, direccion, calle, numero, localidad, provincia, cod_postal, es_principal)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (ent_id, nombre, tipo, tercero_id, direccion, calle, numero, localidad, provincia, cod_postal, es_principal))
                flash(f"Depósito '{nombre}' creado con éxito.", "success")
                return redirect(url_for('stock.depositos_lista'))
        except Exception as e:
            flash(f"Error al crear: {e}", "danger")
            
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, cuit FROM erp_terceros WHERE enterprise_id = %s AND activo = 1 ORDER BY nombre", (ent_id,))
        terceros = cursor.fetchall()
        
    provincias = GeorefService.get_provincias()
    return render_template('stock/deposito_form.html', provincias=provincias, terceros=terceros)

@stock_bp.route('/depositos/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def deposito_editar(id):
    """Editar depósito existente"""
    ent_id = g.user['enterprise_id']
    
    with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            nombre = request.form['nombre']
            tipo = request.form.get('tipo', 'INTERNO')
            tercero_id = request.form.get('tercero_id')
            if not tercero_id: tercero_id = None
            
            calle = request.form.get('calle', '')
            numero = request.form.get('numero', '')
            localidad = request.form.get('localidad', '')
            provincia = request.form.get('provincia', '')
            cod_postal = request.form.get('cod_postal', '')
            direccion = f"{calle} {numero} - {localidad}, {provincia}"
            es_principal = 1 if 'es_principal' in request.form else 0
            activo = 1 if 'activo' in request.form else 0

            try:
                if es_principal:
                    cursor.execute("UPDATE stk_depositos SET es_principal = 0 WHERE enterprise_id = %s", (ent_id,))
                
                cursor.execute("""
                    UPDATE stk_depositos SET 
                        nombre=%s, tipo=%s, tercero_id=%s, direccion=%s, calle=%s, numero=%s, localidad=%s, 
                        provincia=%s, cod_postal=%s, es_principal=%s, activo=%s
                    WHERE id=%s AND enterprise_id=%s
                """, (nombre, tipo, tercero_id, direccion, calle, numero, localidad, provincia, cod_postal, es_principal, activo, id, ent_id))
                flash("Depósito actualizado.", "success")
                return redirect(url_for('stock.depositos_lista'))
            except Exception as e:
                flash(f"Error al actualizar: {e}", "danger")

        cursor.execute("SELECT * FROM stk_depositos WHERE id = %s AND enterprise_id = %s", (id, ent_id))
        deposito = cursor.fetchone()
        if not deposito:
            flash("Depósito no encontrado.", "danger")
            return redirect(url_for('stock.depositos_lista'))
            
        cursor.execute("SELECT id, nombre, cuit FROM erp_terceros WHERE enterprise_id = %s AND activo = 1 ORDER BY nombre", (ent_id,))
        terceros = cursor.fetchall()
        
    provincias = GeorefService.get_provincias()
    return render_template('stock/deposito_form.html', deposito=deposito, provincias=provincias, terceros=terceros)
@stock_bp.route('/tipos')
@login_required
def tipos_articulo():
    """ABM de Tipos de Artículo con Configuración de Servicios"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Obtener Tipos
            cursor.execute("""
                SELECT t.*, 
                       (CASE WHEN t.usa_api_libros = 1 THEN 'Open Library [Legacy]' ELSE 'Nativo' END) as modo_legacy
                FROM stk_tipos_articulo t 
                WHERE t.enterprise_id = %s OR t.enterprise_id = 0
            """, (ent_id,))
            tipos = cursor.fetchall()
            
            # 2. Obtener Servicios Configurados para cada tipo
            for t in tipos:
                cursor.execute("""
                    SELECT s.id, s.nombre, s.tipo_servicio, s.modo_captura, s.url_objetivo
                    FROM stk_tipos_articulo_servicios tas
                    JOIN sys_external_services s ON tas.servicio_id = s.id
                    WHERE tas.tipo_articulo_id = %s AND tas.enterprise_id = %s AND tas.es_primario = 1
                """, (t['id'], ent_id))
                svc = cursor.fetchone()
                if svc:
                    t['servicio_activo'] = svc['nombre']
                    t['servicio_activo_id'] = svc['id']
                    t['modo_captura'] = svc['modo_captura']
                    t['url_objetivo'] = svc['url_objetivo']
                else:
                    t['servicio_activo'] = "Nativo"
                    t['servicio_activo_id'] = "native"
                    t['modo_captura'] = 'NATIVE'
                    t['url_objetivo'] = None

            # 3. Obtener lista de servicios disponibles para el modal
            cursor.execute("SELECT id, nombre, tipo_servicio, modo_captura, url_objetivo FROM sys_external_services WHERE enterprise_id = %s AND activo = 1", (ent_id,))
            servicios_disponibles = cursor.fetchall()

        return render_template('stock/tipos_articulo.html', tipos=tipos, servicios=servicios_disponibles)
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.dashboard'))

@stock_bp.route('/tipos/guardar', methods=['POST'])
@login_required
def tipos_articulo_guardar():
    """Guardar o actualizar Tipo de Artículo y su Servicio Asociado"""
    ent_id = g.user['enterprise_id']
    t_id = request.form.get('id')
    nombre = request.form['nombre']
    descripcion = request.form.get('descripcion', '')
    naturaleza = request.form.get('naturaleza', 'PRODUCTO')
    servicio_id = request.form.get('servicio_id') # ID from sys_external_services or empty for Native
    custom_schema = request.form.get('custom_fields_schema', '')
    
    # Legacy flags for compatibility
    usa_api = 1 if servicio_id else 0 
    
    try:
        with get_db_cursor() as cursor:
            new_id = t_id
            
            if t_id:
                # REGLA DE INTEGRIDAD: No permitir eliminar atributos si ya fueron usados en operaciones
                try:
                    cursor.execute("SELECT custom_fields_schema FROM stk_tipos_articulo WHERE id = %s", (t_id,))
                    old_data = cursor.fetchone()
                    old_schema_str = old_data[0] if old_data else '[]'
                    
                    old_schema = json.loads(old_schema_str or '[]')
                    new_schema = json.loads(custom_schema or '[]')
                    
                    old_keys = set(f.get('name') for f in old_schema if f.get('name'))
                    new_keys = set(f.get('name') for f in new_schema if f.get('name'))
                    
                    removed = old_keys - new_keys
                    if removed:
                        # Verificar si hay artículos de este tipo con operaciones
                        # Revisamos comprobantes, movimientos de stock, transferencias y solicitudes de devolución
                        cursor.execute("""
                            SELECT 
                                (SELECT COUNT(*) FROM erp_comprobantes_detalle d 
                                 JOIN stk_articulos a ON d.articulo_id = a.id 
                                 WHERE a.tipo_articulo_id = %s) +
                                (SELECT COUNT(*) FROM stk_movimientos_detalle d 
                                 JOIN stk_articulos a ON d.articulo_id = a.id 
                                 WHERE a.tipo_articulo_id = %s) +
                                (SELECT COUNT(*) FROM stk_items_transferencia d 
                                 JOIN stk_articulos a ON d.articulo_id = a.id 
                                 WHERE a.tipo_articulo_id = %s) +
                                (SELECT COUNT(*) FROM stk_devoluciones_solicitudes_det d 
                                 JOIN stk_articulos a ON d.articulo_id = a.id 
                                 WHERE a.tipo_articulo_id = %s)
                            as total_ops
                        """, (t_id, t_id, t_id, t_id))
                        res_ops = cursor.fetchone()
                        if res_ops and res_ops[0] > 0:
                            flash(f"Error: Los atributos {list(removed)} no pueden eliminarse porque el tipo de artículo ya tiene operaciones registradas. Puede modificar su etiqueta o contenido, pero el atributo debe permanecer.", "danger")
                            return redirect(url_for('stock.tipos_articulo'))
                except Exception as ve:
                    print(f"Error en validación de integridad de esquema: {ve}")

                # Logica unificada (Permitimos editar tipos propios y de sistema si se requiere globalmente)
                cursor.execute("""
                    UPDATE stk_tipos_articulo SET nombre=%s, descripcion=%s, usa_api_libros=%s, custom_fields_schema=%s, naturaleza=%s
                    WHERE id=%s AND (enterprise_id=%s OR enterprise_id=0)
                """, (nombre, descripcion, usa_api, custom_schema, naturaleza, t_id, ent_id))
            else:
                cursor.execute("""
                    INSERT INTO stk_tipos_articulo (enterprise_id, nombre, descripcion, usa_api_libros, custom_fields_schema, naturaleza)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (ent_id, nombre, descripcion, usa_api, custom_schema, naturaleza))
                new_id = cursor.lastrowid
            
            # Update Service Link configuration
            
            # Update Service Link configuration
            # 1. Clear existing primary link
            cursor.execute("DELETE FROM stk_tipos_articulo_servicios WHERE tipo_articulo_id = %s AND enterprise_id = %s", (new_id, ent_id))
            
            # 2. Add new link if service selected
            if servicio_id and servicio_id != 'native':
                cursor.execute("""
                    INSERT INTO stk_tipos_articulo_servicios (enterprise_id, tipo_articulo_id, servicio_id, es_primario)
                    VALUES (%s, %s, %s, 1)
                """, (ent_id, new_id, servicio_id))
                
        flash("Configuración de Tipo de Artículo guardada.", "success")
    except Exception as e:
        flash(f"Error: {e}", "danger")
    return redirect(url_for('stock.tipos_articulo'))

# --- API ---

@stock_bp.route('/api/articulos/search')
@login_required
def api_search():
    try:
        q = request.args.get('q', '')
        nombre = request.args.get('nombre', '')
        autor = request.args.get('autor', '')
        genero = request.args.get('genero', '')
        isbn = request.args.get('isbn', '')
        editorial = request.args.get('editorial', '')
        precio_min = request.args.get('precio_min', '')
        precio_max = request.args.get('precio_max', '')
        fecha_desde = request.args.get('fecha_desde', '')
        fecha_hasta = request.args.get('fecha_hasta', '')
        paginas_min = request.args.get('paginas_min', '')
        paginas_max = request.args.get('paginas_max', '')
        
        query = """
            SELECT id, nombre, modelo as autor, JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.genero')) as genero, 
                   codigo as isbn, precio_venta as precio, 
                   IFNULL((SELECT SUM(cantidad) FROM stk_existencias WHERE articulo_id = stk_articulos.id AND enterprise_id = stk_articulos.enterprise_id), 0) as numero_ejemplares, 
                   marca as editorial, JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.fecha_publicacion')) as fecha_publicacion, 
                   JSON_EXTRACT(metadata_json, '$.paginas') as numero_paginas,
                   JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.cover_url')) as cover_url,
                   lengua, origen, JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.descripcion')) as descripcion,
                   tipo_articulo_id
            FROM stk_articulos 
            WHERE enterprise_id = %s
        """
        params = [g.user['enterprise_id']]
        
        if q:
            for token in q.split():
                query += " AND (nombre LIKE %s OR modelo LIKE %s OR codigo LIKE %s OR marca LIKE %s)"
                p = f"%{token}%"
                params.extend([p, p, p, p])

        if nombre:
            query += " AND nombre LIKE %s"
            params.append(f"%{nombre}%")
        if autor:
            query += " AND modelo LIKE %s"
            params.append(f"%{autor}%")
        if genero:
            query += " AND JSON_EXTRACT(metadata_json, '$.genero') LIKE %s"
            params.append(f"%{genero}%")
        if isbn:
            query += " AND codigo LIKE %s"
            params.append(f"%{isbn}%")
        if editorial:
            query += " AND marca LIKE %s"
            params.append(f"%{editorial}%")
        
        if precio_min:
            query += " AND precio_venta >= %s"
            params.append(float(precio_min))
        if precio_max:
            query += " AND precio_venta <= %s"
            params.append(float(precio_max))
        
        if fecha_desde:
            query += " AND JSON_EXTRACT(metadata_json, '$.fecha_publicacion') >= %s"
            params.append(fecha_desde)
        if fecha_hasta:
            query += " AND JSON_EXTRACT(metadata_json, '$.fecha_publicacion') <= %s"
            params.append(fecha_hasta)
        
        if paginas_min:
            query += " AND JSON_EXTRACT(metadata_json, '$.paginas') >= %s"
            params.append(int(paginas_min))
        if paginas_max:
            query += " AND JSON_EXTRACT(metadata_json, '$.paginas') <= %s"
            params.append(int(paginas_max))
            
        from flask import jsonify
        with get_db_cursor(dictionary=True) as cursor:
            # Query optimizada con CTEs para evitar N+1
            optimized_query = f"""
                WITH StockCounts AS (
                    SELECT articulo_id, SUM(cantidad) as total
                    FROM stk_existencias
                    WHERE enterprise_id = %s
                    GROUP BY articulo_id
                ),
                LoanCounts AS (
                    SELECT libro_id, COUNT(*) as prestados
                    FROM prestamos
                    WHERE fecha_devolucion_real IS NULL AND enterprise_id = %s
                    GROUP BY libro_id
                ),
                PendingCounts AS (
                    SELECT libro_id, SUM(cantidad) as pendientes
                    FROM movimientos_pendientes
                    WHERE estado = 'pendiente' AND (tipo = 'egreso' OR tipo = 'baja') AND enterprise_id = %s
                    GROUP BY libro_id
                ),
                EnCaminoCounts AS (
                    SELECT libro_id, SUM(cantidad) as en_camino
                    FROM movimientos_pendientes
                    WHERE estado = 'pendiente' AND tipo = 'compra' AND enterprise_id = %s
                    GROUP BY libro_id
                )
                SELECT 
                    l.id, l.nombre, l.modelo as autor, l.codigo as isbn, l.precio_venta as precio,
                    l.marca as editorial, l.lengua, l.origen, l.tipo_articulo_id,
                    JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.genero')) as genero,
                    JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.cover_url')) as cover_url,
                    JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.descripcion')) as descripcion,
                    JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.fecha_publicacion')) as fecha_publicacion,
                    JSON_EXTRACT(l.metadata_json, '$.paginas') as numero_paginas,
                    IFNULL(s.total, 0) as numero_ejemplares,
                    IFNULL(loan.prestados, 0) as prestados,
                    IFNULL(pend.pendientes, 0) as pendientes,
                    IFNULL(camino.en_camino, 0) as en_camino,
                    (IFNULL(s.total, 0) - IFNULL(loan.prestados, 0) - IFNULL(pend.pendientes, 0)) as disponibles
                FROM stk_articulos l
                LEFT JOIN StockCounts s ON l.id = s.articulo_id
                LEFT JOIN LoanCounts loan ON l.id = loan.libro_id
                LEFT JOIN PendingCounts pend ON l.id = pend.libro_id
                LEFT JOIN EnCaminoCounts camino ON l.id = camino.libro_id
                WHERE l.enterprise_id = %s
            """
            
            # Replicamos los filtros sobre la query optimizada
            final_params = [g.user['enterprise_id']] * 5
            
            sql_filters = ""
            if q:
                for token in q.split():
                    sql_filters += " AND (l.nombre LIKE %s OR l.modelo LIKE %s OR l.codigo LIKE %s OR l.marca LIKE %s)"
                    p = f"%{token}%"
                    final_params.extend([p, p, p, p])
            
            # (Resto de filtros nombre, autor, etc. simplificados para velocidad)
            if nombre:
                sql_filters += " AND l.nombre LIKE %s"
                final_params.append(f"%{nombre}%")
            if autor:
                sql_filters += " AND l.modelo LIKE %s"
                final_params.append(f"%{autor}%")
            if isbn:
                sql_filters += " AND l.codigo LIKE %s"
                final_params.append(f"%{isbn}%")
            
            # Ejecutar query completa
            cursor.execute(optimized_query + sql_filters + " LIMIT 50", final_params)
            libros = cursor.fetchall()

            for l in libros:
                # Fix next return date only for those without stock
                if l['disponibles'] <= 0 and l['prestados'] > 0:
                    cursor.execute("""
                        SELECT MIN(fecha_devol_esperada) FROM prestamos 
                        WHERE libro_id = %s AND fecha_devolucion_real IS NULL AND enterprise_id = %s
                    """, (l['id'], g.user['enterprise_id']))
                    res = cursor.fetchone()
                    l['proxima_devolucion'] = str(res['MIN(fecha_devol_esperada)']) if res and res['MIN(fecha_devol_esperada)'] else None
                else:
                    l['proxima_devolucion'] = None
                
                # Limpiar descripciones JSON en los resultados de búsqueda
                if l.get('descripcion') and l['descripcion'].startswith('{'):
                    try:
                        d_obj = json.loads(l['descripcion'])
                        if isinstance(d_obj, dict) and 'value' in d_obj:
                            l['descripcion'] = d_obj['value']
                    except: pass
            
            return jsonify({'libros': libros})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@stock_bp.route('/api/prestamos/libro/<int:id>')
@login_required
def api_prestamos_libro(id):
    try:
        from flask import jsonify
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT u.nombre, u.apellido, p.fecha_devol_esperada 
                FROM prestamos p 
                JOIN usuarios u ON p.usuario_id = u.id AND u.enterprise_id = p.enterprise_id
                WHERE p.libro_id = %s AND p.fecha_devolucion_real IS NULL AND p.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            rows = cursor.fetchall()
            res = [{'usuario': f"{r[0]} {r[1]}", 'fecha': str(r[2])} for r in rows]
            return jsonify({'prestamos': res})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@stock_bp.route('/api/stock/stats')
@login_required
@permission_required('stock_view')
def api_stock_stats():
    try:
        from flask import jsonify
        with get_db_cursor() as cursor:
            cursor.execute("SELECT IFNULL(SUM(a.cantidad), 0) FROM stk_movimientos_detalle a JOIN stk_movimientos m ON a.movimiento_id = m.id WHERE m.enterprise_id = %s", (g.user['enterprise_id'],))
            total_movs = cursor.fetchone()[0]
            return jsonify({'total_movimientos': total_movs})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@stock_bp.route('/api/enrichment/service-efficiency')
@login_required
def api_service_efficiency():
    """API endpoint para obtener estadísticas de eficiencia de servicios de enriquecimiento"""
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT service_name, hits_count, fields_provided, ebooks_provided
                FROM service_efficiency 
                ORDER BY fields_provided DESC, hits_count DESC
                LIMIT 10
            """)
            services = cursor.fetchall()
            
            # Calcular promedio de campos por hit
            for s in services:
                s['avg_fields'] = round(s['fields_provided'] / s['hits_count'], 2) if s['hits_count'] > 0 else 0
            
            return jsonify({'services': services})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@stock_bp.route('/download/ebook/<int:articulo_id>')
@login_required
def download_ebook(articulo_id):
    """Descargar archivo digital asociado al artículo"""
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Verificar enterprise_id por seguridad
            cursor.execute("""
                SELECT stk_archivos_digitales.contenido, stk_archivos_digitales.formato, 
                       stk_archivos_digitales.nombre_archivo, stk_articulos.nombre, stk_articulos.codigo 
                FROM stk_archivos_digitales
                JOIN stk_articulos ON stk_archivos_digitales.articulo_id = stk_articulos.id
                WHERE stk_archivos_digitales.articulo_id = %s AND stk_articulos.enterprise_id = %s
            """, (articulo_id, g.user['enterprise_id']))
            
            file_data = cursor.fetchone()
            
            if not file_data:
                flash("No se encontró archivo digital para este libro.", "warning")
                return redirect(url_for('stock.articulos'))
                
            from flask import send_file
            import io
            import re
            
            # Determinar extensión correcta
            mime = str(file_data['formato']).lower()
            ext = ''
            if 'pdf' in mime: ext = '.pdf'
            elif 'epub' in mime: ext = '.epub'
            elif 'text' in mime or 'txt' in mime: ext = '.txt'
            
            # Construir nombre descriptivo seguro
            clean_title = re.sub(r'[^\w\s-]', '', file_data['nombre'] or 'Libro').strip().replace(' ', '_')
            # Limitar largo título
            clean_title = clean_title[:50]
            isbn = file_data['codigo'] or 'SinISBN'
            
            fname = file_data['nombre_archivo']
            # Si no tiene nombre o no termina con la extensión correcta (y tenemos extensión detectada), forzar nuevo nombre
            if ext and (not fname or not fname.lower().endswith(ext)):
                fname = f"{clean_title}_{isbn}{ext}"
            
            return send_file(
                io.BytesIO(file_data['contenido']),
                mimetype=f"application/{file_data['formato']}" if '/' not in file_data['formato'] else file_data['formato'],
                as_attachment=True,
                download_name=fname
            )
    except Exception as e:
        flash(f"Error al descargar archivo: {str(e)}", "error")
        return redirect(url_for('stock.articulos'))

@stock_bp.route('/api/series/<int:articulo_id>')
@login_required
def api_get_series(articulo_id):
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        # Recuperar series con info básica. Podríamos hacer JOIN con ubicaciones si existiera tabla de ubicaciones definida.
        cursor.execute("""
            SELECT id, numero_serie, estado, ubicacion_id
            FROM stk_numeros_serie
            WHERE articulo_id = %s AND enterprise_id = %s
            ORDER BY estado ASC, numero_serie ASC
        """, (articulo_id, ent_id))
        series = cursor.fetchall()
    return jsonify(series)

# --- TRANSFERENCIAS Y COT ---

@stock_bp.route('/transferencias')
@login_required
def transferencias_lista():
    """Lista de transferencias entre depósitos"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT t.*, 
                       do.nombre as origen_nombre, 
                       dd.nombre as destino_nombre,
                       dd.tipo as destino_tipo
                FROM stk_transferencias t
                JOIN stk_depositos do ON t.origen_id = do.id
                JOIN stk_depositos dd ON t.destino_id = dd.id
                WHERE t.enterprise_id = %s
                ORDER BY t.fecha DESC
            """, (ent_id,))
            transferencias = cursor.fetchall()
        return render_template('stock/transferencias.html', transferencias=transferencias)
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.dashboard'))

@stock_bp.route('/transferencias/nueva')
@login_required
def transferencia_nueva():
    """Formulario para nueva transferencia"""
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, tipo FROM stk_depositos WHERE enterprise_id = %s AND activo = 1 ORDER BY nombre", (ent_id,))
        depositos = cursor.fetchall()
        cursor.execute("SELECT id, nombre, codigo FROM stk_articulos WHERE enterprise_id = %s ORDER BY nombre", (ent_id,))
        articulos = cursor.fetchall()
        cursor.execute("SELECT id, nombre FROM stk_logisticas WHERE enterprise_id = %s AND activo = 1 ORDER BY nombre", (ent_id,))
        logisticas = cursor.fetchall()
    return render_template('stock/transferencia_form.html', 
                           depositos=depositos, 
                           articulos=articulos, 
                           logisticas=logisticas)

@stock_bp.route('/api/transferencia/guardar', methods=['POST'])
@login_required
@atomic_transaction('stock', severity=8, impact_category='Financial')
def api_guardar_transferencia():
    """Guardar cabecera e items de una transferencia"""
    data = request.json
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO stk_transferencias (
                    enterprise_id, origen_id, destino_id, logistica_id, tipo_transporte, 
                    destino_final_direccion, motivo, patente_vehiculo, usuario_id, estado
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDIENTE')
            """, (
                ent_id, 
                data['origen_id'], 
                data['destino_id'], 
                data.get('logistica_id'), 
                data.get('tipo_transporte', 'PROPIO'),
                data.get('destino_final_direccion'),
                data.get('observaciones'), 
                data.get('patente'), 
                g.user['id']
            ))
            trans_id = cursor.lastrowid
            
            for item in data['items']:
                cursor.execute("""
                    INSERT INTO stk_items_transferencia (enterprise_id, transferencia_id, articulo_id, cantidad)
                    VALUES (%s, %s, %s, %s)
                """, (ent_id, trans_id, int(item['id']), float(item['cant'])))
            
            return jsonify({'success': True, 'id': trans_id})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@stock_bp.route('/api/transferencia/<int:id>/solicitar-cot', methods=['POST'])
@login_required
def api_solicitar_cot(id):
    """Simulación de solicitud de COT a ARBA/AGIP"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM stk_transferencias WHERE id = %s AND enterprise_id = %s", (id, ent_id))
            trans = cursor.fetchone()
            if not trans: return jsonify({'success': False, 'message': 'No encontrada'})
            
            import random, string
            fake_cot = f"{datetime.now().strftime('%Y%m%d')}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
            
            cursor.execute("UPDATE stk_transferencias SET cot_numero = %s, cot_estado = 'ACTIVO' WHERE id = %s", (fake_cot, id))
            return jsonify({'success': True, 'cot_numero': fake_cot})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@stock_bp.route('/api/transferencia/<int:id>/despachar', methods=['POST'])
@login_required
@atomic_transaction('stock', severity=8, impact_category='Financial')
def api_despachar_transferencia(id):
    """Ejecutar el movimiento físico de stock (Salida)"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM stk_transferencias WHERE id = %s AND enterprise_id = %s", (id, ent_id))
            trans = cursor.fetchone()
            if trans['estado'] != 'PENDIENTE': return jsonify({'success': False, 'message': 'Ya fue despachada'})
            
            cursor.execute("SELECT * FROM stk_items_transferencia WHERE transferencia_id = %s", (id,))
            items = cursor.fetchall()
            
            for item in items:
                cursor.execute("""
                    UPDATE stk_existencias SET cantidad = cantidad - %s, last_updated = NOW()
                    WHERE deposito_id = %s AND articulo_id = %s AND enterprise_id = %s
                """, (item['cantidad'], trans['origen_id'], item['articulo_id'], ent_id))
            
            cursor.execute("UPDATE stk_transferencias SET estado = 'EN_TRANSITO' WHERE id = %s", (id))
            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


# --- INVENTARIOS Y AUDITORIA ---

@stock_bp.route('/inventarios')
@login_required
def inventarios_lista():
    """Listado de controles de inventario"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT i.*, d.nombre as deposito_nombre,
                       (SELECT COUNT(*) FROM stk_items_inventario ii WHERE ii.inventario_id = i.id) as total_items,
                       (SELECT COUNT(*) FROM stk_items_inventario ii WHERE ii.inventario_id = i.id AND ii.stock_fisico > 0) as items_contados
                FROM stk_inventarios i
                JOIN stk_depositos d ON i.deposito_id = d.id
                WHERE i.enterprise_id = %s
                ORDER BY i.fecha_inicio DESC
            """, (ent_id,))
            inventarios = cursor.fetchall()
            for inv in inventarios:
                inv['progreso'] = round((inv['items_contados'] / inv['total_items'] * 100), 1) if inv['total_items'] > 0 else 0
                
            cursor.execute("SELECT id, nombre FROM stk_depositos WHERE enterprise_id = %s AND activo = 1", (ent_id,))
            depositos = cursor.fetchall()
            
        return render_template('stock/inventario.html', inventarios=inventarios, depositos=depositos)
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.dashboard'))

@stock_bp.route('/api/inventario/crear', methods=['POST'])
@login_required
def api_inventario_crear():
    """Iniciar una nueva auditoría de inventario"""
    ent_id = g.user['enterprise_id']
    deposito_id = request.form['deposito_id']
    tipo = request.form['tipo']
    criteria = request.form.get('criteria')
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO stk_inventarios (enterprise_id, deposito_id, tipo, estado, responsable_id, criteria_json)
                VALUES (%s, %s, %s, 'BORRADOR', %s, %s)
            """, (ent_id, deposito_id, tipo, g.user['id'], criteria))
            inv_id = cursor.lastrowid
            
            # Poblar items automáticamente basados en el tipo/criterio
            sql_poblar = """
                INSERT INTO stk_items_inventario (enterprise_id, inventario_id, articulo_id, stock_sistema)
                SELECT %s, %s, e.articulo_id, e.cantidad
                FROM stk_existencias e
                JOIN stk_articulos a ON e.articulo_id = a.id
                WHERE e.deposito_id = %s AND e.enterprise_id = %s
            """
            params = [ent_id, inv_id, deposito_id, ent_id]
            if tipo == 'DIRIGIDO' and criteria:
                # Simplificación: criteria podría ser marca o categoría
                sql_poblar += " AND (a.marca = %s OR a.categoria_id = %s)"
                params.extend([criteria, criteria])
            elif tipo == 'CICLICO':
                # Tomar 20 artículos al azar para control cíclico
                sql_poblar += " ORDER BY RAND() LIMIT 20"
            
            cursor.execute(sql_poblar, tuple(params))
            cursor.execute("UPDATE stk_inventarios SET estado = 'EN_PROCESO' WHERE id = %s", (inv_id,))
            
        flash("Inventario iniciado correctamente.", "success")
        return redirect(url_for('stock.inventario_toma', id=inv_id))
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.inventarios_lista'))

@stock_bp.route('/inventario/toma/<int:id>')
@login_required
def inventario_toma(id):
    """Página de toma de inventario"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT i.*, d.nombre as deposito_nombre
                FROM stk_inventarios i
                JOIN stk_depositos d ON i.deposito_id = d.id
                WHERE i.id = %s AND i.enterprise_id = %s
            """, (id, ent_id))
            inv = cursor.fetchone()
            
            cursor.execute("""
                SELECT ii.*, a.nombre as articulo_nombre, a.codigo
                FROM stk_items_inventario ii
                JOIN stk_articulos a ON ii.articulo_id = a.id
                WHERE ii.inventario_id = %s
                ORDER BY a.nombre ASC
            """, (id,))
            items = cursor.fetchall()
            
            # Re-calcular progreso para la cabecera
            total = len(items)
            contados = sum(1 for x in items if float(x['stock_fisico']) > 0)
            inv['progreso'] = round((contados / total * 100), 1) if total > 0 else 0
            
        return render_template('stock/inventario_toma.html', inv=inv, items=items)
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.inventarios_lista'))

@stock_bp.route('/api/inventario/item/<int:id>/update', methods=['POST'])
@login_required
def api_inventario_item_update(id):
    """Actualizar conteo físico de un item de inventario"""
    fisico = request.json.get('fisico')
    try:
        with get_db_cursor() as cursor:
            cursor.execute("UPDATE stk_items_inventario SET stock_fisico = %s WHERE id = %s", (fisico, id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@stock_bp.route('/api/inventario/<int:id>/cerrar', methods=['POST'])
@login_required
@atomic_transaction('stock', severity=9, impact_category='Integrity')
def api_inventario_cerrar(id):
    """Cerrar inventario y realizar ajustes de stock automáticos"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM stk_inventarios WHERE id = %s AND enterprise_id = %s", (id, ent_id))
            inv = cursor.fetchone()
            
            cursor.execute("SELECT * FROM stk_items_inventario WHERE inventario_id = %s", (id,))
            items = cursor.fetchall()
            
            for item in items:
                # Ajustar stock del ERP para que coincida con el físico
                cursor.execute("""
                    UPDATE stk_existencias 
                    SET cantidad = %s, last_updated = NOW()
                    WHERE deposito_id = %s AND articulo_id = %s AND enterprise_id = %s
                """, (item['stock_fisico'], inv['deposito_id'], item['articulo_id'], ent_id))
                
                cursor.execute("UPDATE stk_items_inventario SET ajustado = 1 WHERE id = %s", (item['id'],))
            
            cursor.execute("UPDATE stk_inventarios SET estado = 'CERRADO', fecha_cierre = NOW() WHERE id = %s", (id,))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# --- LOGISTICA ---

@stock_bp.route('/logisticas')
@login_required
def logisticas_lista():
    """Listado de empresas logísticas"""
    ent_id = g.user['enterprise_id']
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM stk_logisticas WHERE enterprise_id = %s ORDER BY nombre", (ent_id,))
            logisticas = cursor.fetchall()
        return render_template('stock/logisticas.html', logisticas=logisticas)
    except Exception as e:
        flash(f"Error: {e}", "danger")
        return redirect(url_for('stock.dashboard'))

@stock_bp.route('/logistica/nueva', methods=['GET', 'POST'])
@login_required
def logistica_nueva():
    """Crear nueva empresa logística"""
    ent_id = g.user['enterprise_id']
    if request.method == 'POST':
        nombre = request.form['nombre']
        cuit = request.form.get('cuit', '')
        calle = request.form.get('calle', '')
        numero = request.form.get('numero', '')
        localidad = request.form.get('localidad', '')
        provincia = request.form.get('provincia', '')
        email = request.form.get('email', '')
        telefono = request.form.get('telefono', '')
        activo = 1 if 'activo' in request.form else 0
        cuit = format_cuit(cuit)
        direccion = f"{calle} {numero} - {localidad}, {provincia}"

        try:
            with get_db_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO stk_logisticas (enterprise_id, nombre, cuit, calle, numero, localidad, provincia, direccion, email, telefono, activo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (ent_id, nombre, cuit, calle, numero, localidad, provincia, direccion, email, telefono, activo))
                flash(f"Logística '{nombre}' creada.", "success")
                return redirect(url_for('stock.logisticas_lista'))
        except Exception as e:
            flash(f"Error al crear: {e}", "danger")

    provincias = GeorefService.get_provincias()
    return render_template('stock/logistica_form.html', provincias=provincias)

@stock_bp.route('/logistica/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def logistica_editar(id):
    """Editar empresa logística"""
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            nombre = request.form['nombre']
            cuit = request.form.get('cuit', '')
            calle = request.form.get('calle', '')
            numero = request.form.get('numero', '')
            localidad = request.form.get('localidad', '')
            provincia = request.form.get('provincia', '')
            email = request.form.get('email', '')
            telefono = request.form.get('telefono', '')
            activo = 1 if 'activo' in request.form else 0
            cuit = format_cuit(cuit)
            direccion = f"{calle} {numero} - {localidad}, {provincia}"

            try:
                cursor.execute("""
                    UPDATE stk_logisticas 
                    SET nombre=%s, cuit=%s, calle=%s, numero=%s, localidad=%s, provincia=%s, direccion=%s, email=%s, telefono=%s, activo=%s
                    WHERE id=%s AND enterprise_id=%s
                """, (nombre, cuit, calle, numero, localidad, provincia, direccion, email, telefono, activo, id, ent_id))
                flash(f"Logística '{nombre}' actualizada.", "success")
                return redirect(url_for('stock.logisticas_lista'))
            except Exception as e:
                flash(f"Error al actualizar: {e}", "danger")

        cursor.execute("SELECT * FROM stk_logisticas WHERE id = %s AND enterprise_id = %s", (id, ent_id))
        logistica = cursor.fetchone()
        
    provincias = GeorefService.get_provincias()
    return render_template('stock/logistica_form.html', logistica=logistica, provincias=provincias)


# ----------------------------------------------------------------------
# FASE 1.1: GTIN / MULTI-BARCODES (Trazabilidad Internacional)
# ----------------------------------------------------------------------

@stock_bp.route('/api/articulos/<int:articulo_id>/barcodes', methods=['GET'])
@login_required
@permission_required('view_stock')
def api_get_barcodes(articulo_id):
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, codigo, tipo_codigo, factor_conversion
            FROM stk_articulos_codigos
            WHERE articulo_id = %s AND enterprise_id = %s AND is_active = 1
            ORDER BY id ASC
        """, (articulo_id, ent_id))
        rows = cursor.fetchall()
        
    return jsonify({'success': True, 'barcodes': rows})

@stock_bp.route('/api/articulos/<int:articulo_id>/barcodes', methods=['POST'])
@login_required
@permission_required('books_add')
@atomic_transaction('stock')
def api_add_barcode(articulo_id):
    ent_id = g.user['enterprise_id']
    data = request.json
    cod = data.get('codigo', '').strip()
    tipo = data.get('tipo_codigo', 'GTIN')
    factor = float(data.get('factor_conversion', 1.0))
    
    if not cod:
        return jsonify({'success': False, 'message': 'Código vacío.'})
        
    with get_db_cursor() as cursor:
        try:
            cursor.execute("""
                INSERT INTO stk_articulos_codigos (enterprise_id, articulo_id, codigo, tipo_codigo, factor_conversion)
                VALUES (%s, %s, %s, %s, %s)
            """, (ent_id, articulo_id, cod, tipo, factor))
            b_id = cursor.lastrowid
            
            # Si el código se carga por primera vez y el artículo no tiene EAN base/ISBN, podríamos actualizarlo también (opcional)
            cursor.execute("SELECT isbn FROM stk_articulos WHERE id = %s", (articulo_id,))
            a_row = cursor.fetchone()
            if a_row and not a_row[0]:
                cursor.execute("UPDATE stk_articulos SET isbn = %s WHERE id = %s", (cod, articulo_id))

            return jsonify({'success': True, 'id': b_id})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)})

@stock_bp.route('/api/articulos/barcodes/<int:codigo_id>', methods=['DELETE'])
@login_required
@permission_required('books_add')
def api_delete_barcode(codigo_id):
    ent_id = g.user['enterprise_id']
    with get_db_cursor() as cursor:
        try:
            cursor.execute("DELETE FROM stk_articulos_codigos WHERE id = %s AND enterprise_id = %s", (codigo_id, ent_id))
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)})

# FASE 1.2: Etiquetado de Códigos en Recepción - Multi modo
def _get_default_printer(cursor, ent_id):
    """Helper: obtiene impresora predeterminada o fallback."""
    cursor.execute(
        "SELECT * FROM stk_impresoras_config WHERE enterprise_id = %s AND es_predeterminada = 1 AND activo = 1 LIMIT 1",
        (ent_id,)
    )
    p = cursor.fetchone()
    return p or {
        'ancho_mm': 100.0, 'alto_mm': 50.0, 'marca': 'Generico',
        'nombre': 'Default (Sin Configurar)', 'tipo_conexion': 'BROWSER_DIALOG',
        'ip_red': None, 'puerto_red': 9100, 'nombre_sistema_qz': None
    }


@stock_bp.route('/api/articulos/<int:articulo_id>/print_label', methods=['GET'])
@login_required
@permission_required('view_stock')
def print_label(articulo_id):
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, isbn FROM stk_articulos WHERE id = %s AND enterprise_id = %s", (articulo_id, ent_id))
        articulo = cursor.fetchone()
        
        # --- Seguridad Industrial ---
        cursor.execute("SELECT * FROM stk_articulos_seguridad WHERE articulo_id = %s AND enterprise_id = %s", (articulo_id, ent_id))
        seguridad = cursor.fetchone()
        
        if seguridad and seguridad['pictogramas_json']:
            try:
                seguridad['pictogramas_json'] = json.loads(seguridad['pictogramas_json'])
            except:
                seguridad['pictogramas_json'] = []
        
        printer = _get_default_printer(cursor, ent_id)

    if not articulo:
        return "Artículo no encontrado o acceso denegado.", 404

    data = {
        'ent_id': ent_id,
        'fecha_hoy': datetime.datetime.now().strftime('%d/%m/%Y %H:%M'),
        'printer': printer,
        'seguridad': seguridad
    }
    return render_template('stock/print_label.html', articulo=articulo, data=data)


@stock_bp.route('/api/articulos/<int:articulo_id>/print_zpl', methods=['POST'])
@login_required
@permission_required('view_stock')
def print_zpl_network(articulo_id):
    """Opción C: envía ZPL crudo por socket TCP a una impresora de red."""
    import socket
    ent_id = g.user['enterprise_id']

    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, isbn FROM stk_articulos WHERE id = %s AND enterprise_id = %s", (articulo_id, ent_id))
        articulo = cursor.fetchone()
        printer = _get_default_printer(cursor, ent_id)

    if not articulo:
        return jsonify({'success': False, 'message': 'Artículo no encontrado.'})

    ip = printer.get('ip_red')
    port = int(printer.get('puerto_red') or 9100)

    if not ip:
        return jsonify({'success': False, 'message': 'La impresora predeterminada no tiene IP de red configurada.'})

    # Generar código de barras: usar ISBN si existe, sino SKU interno
    barcode_val = articulo.get('isbn') or f"SKUINT{articulo['id']:06d}"
    ancho = int(float(printer.get('ancho_mm', 100)) * 8)  # puntos: 8dpi/mm aprox.
    alto = int(float(printer.get('alto_mm', 50)) * 8)

    nombre = (articulo['nombre'] or '')[:40]  # ZPL max chars

    zpl = f"""
^XA
^MMT
^PW{ancho}
^LL{alto}
^LS0
^FT20,30^A0N,20,20^FH\^FD{nombre}^FS
^FT20,60^BY2,3,50^BCN,,Y,N^FD{barcode_val}^FS
^FT20,{alto - 10}^A0N,12,12^FDColosal ERP | {datetime.datetime.now().strftime('%d/%m/%Y')}^FS
^XZ
"""

    try:
        with socket.create_connection((ip, port), timeout=5) as sock:
            sock.sendall(zpl.encode('ascii', errors='ignore'))
        return jsonify({'success': True, 'message': f'Etiqueta enviada a {ip}:{port}', 'zpl': zpl})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error de conexión con impresora en red: {str(e)}'})


@stock_bp.route('/api/impresoras/default', methods=['GET'])
@login_required
def api_get_default_printer():
    """Devuelve config de impresora predeterminada para que el JS configure QZ Tray."""
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        printer = _get_default_printer(cursor, ent_id)
    return jsonify({'success': True, 'printer': printer})

# FASE 1.2: CRUD Impresoras
@stock_bp.route('/impresoras', methods=['GET'])
@login_required
@permission_required('system_settings')
def impresoras_lista():
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM stk_impresoras_config WHERE enterprise_id = %s ORDER BY es_predeterminada DESC, id DESC", (ent_id,))
        impresoras = cursor.fetchall()
    return render_template('stock/impresoras.html', impresoras=impresoras)

@stock_bp.route('/impresoras/guardar', methods=['POST'])
@login_required
@permission_required('system_settings')
def impresora_guardar():
    ent_id = g.user['enterprise_id']
    id = request.form.get('id')
    nombre = request.form.get('nombre', '')
    marca = request.form.get('marca', 'Zebra')
    modelo = request.form.get('modelo', '')
    ancho = float(request.form.get('ancho_mm', 100.0))
    alto = float(request.form.get('alto_mm', 0.0)) # 0 means continuous or paper default
    tipo_con = request.form.get('tipo_conexion', 'BROWSER_DIALOG')
    
    # Manejo de IP y Puerto según modo
    ip_red = request.form.get('ip_red') if tipo_con == 'IP_RED' else request.form.get('ip_red_qz')
    puerto_red = request.form.get('puerto_red' if tipo_con == 'IP_RED' else 'puerto_red_qz', 9100)
    qz_name = request.form.get('nombre_sistema_qz') if tipo_con == 'QZ_TRAY_USB' else None
    
    es_predeterminada = 1 if request.form.get('es_predeterminada') else 0
    activo = 1 if request.form.get('activo') else 0
    
    with get_db_cursor() as cursor:
        try:
            if es_predeterminada == 1:
                cursor.execute("UPDATE stk_impresoras_config SET es_predeterminada = 0 WHERE enterprise_id = %s", (ent_id,))
                
            if id:
                cursor.execute("""
                    UPDATE stk_impresoras_config 
                    SET nombre=%s, marca=%s, modelo=%s, ancho_mm=%s, alto_mm=%s, 
                        tipo_conexion=%s, ip_red=%s, puerto_red=%s, nombre_sistema_qz=%s,
                        es_predeterminada=%s, activo=%s
                    WHERE id=%s AND enterprise_id=%s
                """, (nombre, marca, modelo, ancho, alto, tipo_con, ip_red, puerto_red, qz_name, es_predeterminada, activo, id, ent_id))
                flash("Perfil de impresora actualizado.", "success")
            else:
                cursor.execute("""
                    INSERT INTO stk_impresoras_config 
                    (enterprise_id, nombre, marca, modelo, ancho_mm, alto_mm, tipo_conexion, ip_red, puerto_red, nombre_sistema_qz, es_predeterminada, activo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (ent_id, nombre, marca, modelo, ancho, alto, tipo_con, ip_red, puerto_red, qz_name, es_predeterminada, activo))
                flash("Nueva impresora agregada.", "success")
        except Exception as e:
            flash(f"Error al guardar impresora: {e}", "danger")
            
    return redirect(url_for('stock.impresoras_lista'))

@stock_bp.route('/impresoras/eliminar/<int:id>', methods=['POST'])
@login_required
@permission_required('system_settings')
def impresora_eliminar(id):
    ent_id = g.user['enterprise_id']
    with get_db_cursor() as cursor:
        try:
            cursor.execute("DELETE FROM stk_impresoras_config WHERE id = %s AND enterprise_id = %s", (id, ent_id))
            flash("Impresora eliminada.", "success")
        except Exception as e:
            flash(f"Error al eliminar: {e}", "danger")
    return redirect(url_for('stock.impresoras_lista'))


# FASE 2.1: CRUD Balanzas
@stock_bp.route('/balanzas', methods=['GET'])
@login_required
@permission_required('system_settings')
def balanzas_lista():
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM stk_balanzas_config WHERE enterprise_id = %s ORDER BY es_predeterminada DESC, id DESC", (ent_id,))
        balanzas = cursor.fetchall()
    return render_template('stock/balanzas.html', balanzas=balanzas)

@stock_bp.route('/balanzas/guardar', methods=['POST'])
@login_required
@permission_required('system_settings')
def balanza_guardar():
    ent_id = g.user['enterprise_id']
    id = request.form.get('id')
    nombre = request.form.get('nombre', '')
    marca = request.form.get('marca', 'Systel')
    modelo = request.form.get('modelo', '')
    numero_serie = request.form.get('numero_serie', '')
    tipo_conexion = request.form.get('tipo_conexion', 'IP_RED')
    ip_red = request.form.get('ip_red', '')
    puerto_red = int(request.form.get('puerto_red', 9100) or 9100)
    es_predeterminada = 1 if request.form.get('es_predeterminada') else 0
    activo = 1 if request.form.get('activo') else 0
    
    with get_db_cursor() as cursor:
        try:
            if es_predeterminada == 1:
                cursor.execute("UPDATE stk_balanzas_config SET es_predeterminada = 0 WHERE enterprise_id = %s", (ent_id,))
                
            if id:
                cursor.execute("""
                    UPDATE stk_balanzas_config 
                    SET nombre=%s, marca=%s, modelo=%s, numero_serie=%s, tipo_conexion=%s, ip_red=%s, puerto_red=%s, es_predeterminada=%s, activo=%s
                    WHERE id=%s AND enterprise_id=%s
                """, (nombre, marca, modelo, numero_serie, tipo_conexion, ip_red, puerto_red, es_predeterminada, activo, id, ent_id))
                flash("Perfil de balanza actualizado.", "success")
            else:
                cursor.execute("""
                    INSERT INTO stk_balanzas_config 
                    (enterprise_id, nombre, marca, modelo, numero_serie, tipo_conexion, ip_red, puerto_red, es_predeterminada, activo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (ent_id, nombre, marca, modelo, numero_serie, tipo_conexion, ip_red, puerto_red, es_predeterminada, activo))
                flash("Nueva balanza agregada.", "success")
        except Exception as e:
            flash(f"Error al guardar balanza: {e}", "danger")
            
    return redirect(url_for('stock.balanzas_lista'))

@stock_bp.route('/balanzas/eliminar/<int:id>', methods=['POST'])
@login_required
@permission_required('system_settings')
def balanza_eliminar(id):
    ent_id = g.user['enterprise_id']
    with get_db_cursor() as cursor:
        try:
            cursor.execute("DELETE FROM stk_balanzas_config WHERE id = %s AND enterprise_id = %s", (id, ent_id))
            flash("Balanza eliminada.", "success")
        except Exception as e:
            flash(f"Error al eliminar: {e}", "danger")
    return redirect(url_for('stock.balanzas_lista'))


# ======================================================================
# FASE 1.3: SKU DUAL Y CONTROL DE DEVOLUCIONES
# ======================================================================

@stock_bp.route('/api/lookup', methods=['GET'])
@login_required
def sku_dual_lookup():
    """Búsqueda bidireccional: SKU Propietario o SKU Proveedor/GTIN."""
    q = request.args.get('q', '').strip()
    ent_id = g.user['enterprise_id']
    if not q: return jsonify([])

    from utils.barcode_parser import parse_dynamic_barcode

    with get_db_cursor(dictionary=True) as cursor:
        # FASE 2.2: Intento de parseo dinámico (Balanza/EAN-13 Variable)
        parsed = parse_dynamic_barcode(q, ent_id, cursor)
        search_query = q
        found_dynamic = False
        dynamic_value = None

        if parsed:
            # Si es un código de balanza, buscamos por el PLU/SKU extraído
            search_query = parsed['sku_plu']
            found_dynamic = True
            dynamic_value = parsed['valor']

        # Buscar por SKU Propietario (erp-style) o Código de Barras Alias (proveedor)
        cursor.execute("""
            SELECT stk_articulos.id, stk_articulos.nombre, stk_articulos.codigo AS sku_propietario, 
                   stk_articulos_codigos.codigo AS sku_proveedor, stk_articulos_codigos.tipo_codigo, 
                   stk_articulos.requiere_serie, stk_articulos.unidad_medida,
                   p.precio_venta AS precio_actual
            FROM stk_articulos
            LEFT JOIN stk_articulos_codigos ON stk_articulos.id = stk_articulos_codigos.articulo_id 
                 AND stk_articulos_codigos.enterprise_id = stk_articulos.enterprise_id
            LEFT JOIN (
                SELECT articulo_id, enterprise_id, MAX(precio_final) AS precio_venta 
                FROM stk_articulos_precios 
                GROUP BY articulo_id, enterprise_id
            ) p ON p.articulo_id = stk_articulos.id AND p.enterprise_id = stk_articulos.enterprise_id
            WHERE stk_articulos.enterprise_id = %s 
              AND (stk_articulos.codigo = %s OR stk_articulos_codigos.codigo = %s OR stk_articulos.nombre LIKE %s)
            LIMIT 20
        """, (ent_id, search_query, search_query, f'%{search_query}%'))
        results = cursor.fetchall()

        # Inyectar metadatos de balanza si corresponde
        if found_dynamic and results:
            for res in results:
                res['dynamic_barcode'] = True
                res['dynamic_value'] = dynamic_value
                res['dynamic_type'] = parsed['tipo']
                res['original_code'] = q
    
    return jsonify(results)


@stock_bp.route('/api/seriales/validar-devolucion', methods=['POST'])
@login_required
def serial_validar_devolucion():
    """
    Valida si un serial puede ser devuelto por un cliente.
    Fase 1.3: Control de que la devolución sea el mismo serial vendido.
    """
    ent_id = g.user['enterprise_id']
    data = request.json
    numero = data.get('numero_serie', '').strip()
    cliente_id = data.get('cliente_id') # erp_terceros.id
    articulo_id = data.get('articulo_id')

    if not numero or not cliente_id:
        return jsonify({'success': False, 'message': 'Faltan datos (serie o cliente).'})

    with get_db_cursor(dictionary=True) as cursor:
        # 1. Buscar el serial
        cursor.execute("""
            SELECT s.*, a.nombre as articulo_nombre
            FROM stk_numeros_serie s
            JOIN stk_articulos a ON s.articulo_id = a.id
            WHERE s.numero_serie = %s AND s.enterprise_id = %s
        """, (numero, ent_id))
        serie = cursor.fetchone()

        if not serie:
            return jsonify({
                'success': True, 
                'exists': False, 
                'message': 'Serie no encontrada en el sistema. Se registrará como ingreso nuevo por devolución (Legacy Stock).'
            })

        # 2. Validar pertenencia al cliente
        if serie['tercero_id'] and int(serie['tercero_id']) != int(cliente_id):
            return jsonify({
                'success': False,
                'message': f'ALERTA: Esta serie fue vendida a otro cliente (ID {serie["tercero_id"]}). No corresponde a este cliente.'
            })
        
        if serie['estado'] != 'VENDIDO' and serie['estado'] != 'EN_STOCK' :
             return jsonify({
                'success': False,
                'message': f'ALERTA: La serie figura en estado {serie["estado"]}. No puede ser devuelta.'
            })

        return jsonify({
            'success': True,
            'exists': True,
            'articulo_id': serie['articulo_id'],
            'articulo_nombre': serie['articulo_nombre'],
            'message': 'Serie validada correctamente para devolución.'
        })


@stock_bp.route('/api/seriales/procesar-devolucion', methods=['POST'])
@login_required
@permission_required('books_add')
def serial_procesar_devolucion():
    """
    Procesa la devolución física de un serial.
    Actualiza estado y graba LOG con FECHA EFECTIVA del comprobante.
    """
    ent_id = g.user['enterprise_id']
    user_id = g.user['id']
    data = request.json
    numero = data.get('numero_serie', '').strip()
    nc_id = data.get('nc_id') 
    articulo_id = data.get('articulo_id')
    estado_final = data.get('estado', 'EN_STOCK') 

    if not numero or not nc_id or not articulo_id:
        return jsonify({'success': False, 'message': 'Faltan parámetros.'})

    with get_db_cursor() as cursor:
        try:
            # 1. Obtener ID del cliente y FECHA EFECTIVA desde la NC
            cursor.execute("SELECT tercero_id, fecha_emision FROM erp_comprobantes WHERE id = %s", (nc_id,))
            doc_row = cursor.fetchone()
            cliente_id = doc_row[0] if doc_row else None
            fecha_efectiva = doc_row[1] if doc_row else datetime.now().date()

            # 2. Actualizar serial
            cursor.execute("""
                UPDATE stk_numeros_serie 
                SET estado = %s, comprobante_nc_id = %s, fecha_devolucion = %s, comprobante_venta_id = NULL
                WHERE numero_serie = %s AND enterprise_id = %s
            """, (estado_final, nc_id, fecha_efectiva, numero, ent_id))

            if cursor.rowcount == 0:
                cursor.execute("""
                    INSERT INTO stk_numeros_serie (enterprise_id, articulo_id, numero_serie, origen, estado, comprobante_nc_id, fecha_devolucion)
                    VALUES (%s, %s, %s, 'MANUAL_SCAN', %s, %s, %s)
                """, (ent_id, articulo_id, numero, estado_final, nc_id, fecha_efectiva))
            
            cursor.execute("SELECT id FROM stk_numeros_serie WHERE numero_serie = %s AND enterprise_id = %s", (numero, ent_id))
            serie_id = cursor.fetchone()[0]

            # 3. GRABAR MOVIMIENTO CON FECHA EFECTIVA
            cursor.execute("""
                INSERT INTO stk_series_trazabilidad 
                (enterprise_id, serie_id, tipo_evento, fecha_efectiva, tercero_id, comprobante_id, user_id, estado_resultante, notas)
                VALUES (%s, %s, 'DEVOLUCION', %s, %s, %s, %s, %s, 'Reingreso al stock por Devolución')
            """, (ent_id, serie_id, fecha_efectiva, cliente_id, nc_id, user_id, estado_final))
            
            return jsonify({'success': True, 'message': f'Serie {numero} reingresada con fecha {fecha_efectiva}.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error: {e}'})


@stock_bp.route('/api/seriales/procesar-venta', methods=['POST'])
@login_required
@permission_required('books_add')
def serial_procesar_venta():
    """
    Procesa el egreso de un serial por venta (Factura/Remito).
    """
    ent_id = g.user['enterprise_id']
    user_id = g.user['id']
    data = request.json
    numero = data.get('numero_serie', '').strip()
    factura_id = data.get('comprobante_id') 

    if not numero or not factura_id:
        return jsonify({'success': False, 'message': 'Faltan parámetros.'})

    with get_db_cursor() as cursor:
        try:
            # 1. Obtener datos de la factura
            cursor.execute("SELECT tercero_id, fecha_emision FROM erp_comprobantes WHERE id = %s", (factura_id,))
            doc_row = cursor.fetchone()
            cliente_id = doc_row[0] if doc_row else None
            fecha_efectiva = doc_row[1] if doc_row else datetime.now().date()

            # 2. Marcar como VENDIDO
            cursor.execute("""
                UPDATE stk_numeros_serie 
                SET estado = 'VENDIDO', comprobante_venta_id = %s, tercero_id = %s, fecha_egreso = %s, comprobante_nc_id = NULL
                WHERE numero_serie = %s AND enterprise_id = %s
            """, (factura_id, cliente_id, fecha_efectiva, numero, ent_id))

            if cursor.rowcount == 0:
                return jsonify({'success': False, 'message': 'La serie no existe o ya no está disponible.'})

            cursor.execute("SELECT id FROM stk_numeros_serie WHERE numero_serie = %s AND enterprise_id = %s", (numero, ent_id))
            serie_id = cursor.fetchone()[0]

            # 3. Log de Venta
            cursor.execute("""
                INSERT INTO stk_series_trazabilidad 
                (enterprise_id, serie_id, tipo_evento, fecha_efectiva, tercero_id, comprobante_id, user_id, estado_resultante, notas)
                VALUES (%s, %s, 'VENTA', %s, %s, %s, %s, 'VENDIDO', 'Egreso por venta de mercadería')
            """, (ent_id, serie_id, fecha_efectiva, cliente_id, factura_id, user_id))
            
            return jsonify({'success': True, 'message': f'Serie {numero} asignada a la venta con fecha {fecha_efectiva}.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error: {e}'})


@stock_bp.route('/api/seriales/registrar-hecho', methods=['POST'])
@login_required
@permission_required('books_add')
def serial_registrar_hecho():
    """
    Registra un hecho económico o de inventario genérico para un serial.
    Involucra: Traslados, Ajustes, Bajas, etc.
    """
    ent_id = g.user['enterprise_id']
    user_id = g.user['id']
    data = request.json
    
    # Parámetros básicos
    numero = data.get('numero_serie', '').strip()
    tipo_evento = data.get('tipo_evento') # TRASLADO, AJUSTE, BAJA, INGRESO
    fecha_efectiva = data.get('fecha_efectiva') or datetime.now().date()
    
    # Contexto del movimiento
    deposito_id = data.get('deposito_id') 
    tercero_id = data.get('tercero_id') # Ej: Proveedor en un ajuste de fábrica
    comprobante_id = data.get('comprobante_id') 
    referencia_txt = data.get('referencia', '').strip()
    estado_final = data.get('estado') # EN_STOCK, DADO_BAJA, etc.
    notas = data.get('notas', '')

    if not numero or not tipo_evento:
        return jsonify({'success': False, 'message': 'Faltan campos obligatorios (serie y tipo evento).'})

    with get_db_cursor() as cursor:
        try:
            # 1. Buscar o Crear Serie
            cursor.execute("SELECT id, articulo_id FROM stk_numeros_serie WHERE numero_serie = %s AND enterprise_id = %s", (numero, ent_id))
            serie_row = cursor.fetchone()
            
            if not serie_row:
                # Si es un ajuste de ingreso, la creamos
                if tipo_evento == 'INGRESO' or tipo_evento == 'AJUSTE':
                    articulo_id = data.get('articulo_id')
                    if not articulo_id: return jsonify({'success': False, 'message': 'Se requiere articulo_id para registrar serie nueva.'})
                    
                    cursor.execute("""
                        INSERT INTO stk_numeros_serie (enterprise_id, articulo_id, numero_serie, origen, estado)
                        VALUES (%s, %s, %s, 'MANUAL_SCAN', %s)
                    """, (ent_id, articulo_id, numero, estado_final or 'EN_STOCK'))
                    serie_id = cursor.lastrowid
                else:
                    return jsonify({'success': False, 'message': f'La serie {numero} no existe y el evento {tipo_evento} no permite creación.'})
            else:
                serie_id = serie_row[0]
                # Actualizar estado si se provee
                if estado_final:
                    cursor.execute("UPDATE stk_numeros_serie SET estado = %s WHERE id = %s", (estado_final, serie_id))

            # 2. SELLAR EL HECHO EN EL LOG (Pedigree)
            cursor.execute("""
                INSERT INTO stk_series_trazabilidad 
                (enterprise_id, serie_id, tipo_evento, fecha_efectiva, tercero_id, deposito_id, 
                 comprobante_id, referencia_identificador, user_id, estado_resultante, notas)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ent_id, serie_id, tipo_evento, fecha_efectiva, tercero_id, deposito_id, 
                  comprobante_id, referencia_txt, user_id, estado_final, notas))
            
            return jsonify({'success': True, 'message': f'Hecho {tipo_evento} registrado para serie {numero}.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error en trazabilidad: {e}'})


@stock_bp.route('/api/seriales/vincular-caja', methods=['POST'])
@login_required
@permission_required('books_add')
def serial_vincular_caja():
    """
    Agregación: Vincula múltiples seriales (Hijos) a un Serial Contenedor (Padre/Caja).
    Fase 1.4: Master Box Consolidation.
    """
    ent_id = g.user['enterprise_id']
    user_id = g.user['id']
    data = request.json
    
    parent_serie_nro = data.get('parent_serie', '').strip()
    children_series = data.get('children_series', []) # Lista de strings

    if not parent_serie_nro or not children_series:
        return jsonify({'success': False, 'message': 'Se requiere serie de la caja y lista de unidades.'})

    with get_db_cursor() as cursor:
        try:
            # 1. Asegurar existencia de la caja
            cursor.execute("SELECT id, articulo_id FROM stk_numeros_serie WHERE numero_serie = %s AND enterprise_id = %s", (parent_serie_nro, ent_id))
            parent_row = cursor.fetchone()
            if not parent_row:
                return jsonify({'success': False, 'message': f'La caja {parent_serie_nro} no existe.'})
            
            parent_id = parent_row[0]
            articulo_id = parent_row[1]

            # Marcar como contenedor
            cursor.execute("UPDATE stk_numeros_serie SET es_contenedor = 1 WHERE id = %s", (parent_id,))

            # 2. Vincular hijos
            vinculados = 0
            for sn in children_series:
                sn = sn.strip()
                if not sn: continue
                # Actualizar hijo
                cursor.execute("""
                    UPDATE stk_numeros_serie 
                    SET parent_id = %s 
                    WHERE numero_serie = %s AND enterprise_id = %s
                """, (parent_id, sn, ent_id))
                
                if cursor.rowcount > 0:
                    vinculados += 1
                    # Log de trazabilidad para el hijo
                    cursor.execute("SELECT id FROM stk_numeros_serie WHERE numero_serie = %s AND enterprise_id = %s", (sn, ent_id))
                    sid = cursor.fetchone()[0]
                    cursor.execute("""
                        INSERT INTO stk_series_trazabilidad (enterprise_id, serie_id, tipo_evento, user_id, notas)
                        VALUES (%s, %s, 'AJUSTE', %s, %s)
                    """, (ent_id, sid, user_id, f'Vinculado al Master Box {parent_serie_nro}'))
            
            return jsonify({'success': True, 'message': f'Se vincularon {vinculados} unidades a la caja {parent_serie_nro}.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error en vinculación: {e}'})


@stock_bp.route('/api/seriales/desvincular-caja', methods=['POST'])
@login_required
def serial_desvincular_caja():
    """
    Desconsolidación: Libera los seriales contenidos en un Master Box.
    Fase 1.4: Master Box Unpacking.
    """
    ent_id = g.user['enterprise_id']
    user_id = g.user['id']
    parent_serie = request.json.get('parent_serie', '').strip()

    if not parent_serie: return jsonify({'success': False, 'message': 'Serie de caja requerida.'})

    with get_db_cursor() as cursor:
        try:
            cursor.execute("SELECT id FROM stk_numeros_serie WHERE numero_serie = %s AND enterprise_id = %s", (parent_serie, ent_id))
            p_row = cursor.fetchone()
            if not p_row: return jsonify({'success': False, 'message': 'Caja no encontrada.'})
            
            p_id = p_row[0]
            
            # Grabar log antes de desvincular
            cursor.execute("""
                INSERT INTO stk_series_trazabilidad (enterprise_id, serie_id, tipo_evento, user_id, notas)
                SELECT enterprise_id, id, 'AJUSTE', %s, %s FROM stk_numeros_serie WHERE parent_id = %s
            """, (user_id, f'Desvinculado del Master Box {parent_serie} (Desconsolidación)', p_id))

            # Desvincular
            cursor.execute("UPDATE stk_numeros_serie SET parent_id = NULL WHERE parent_id = %s", (p_id,))
            
            return jsonify({'success': True, 'message': 'Contenido de la caja liberado individualmente.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error: {e}'})


@stock_bp.route('/api/seriales/<int:serie_id>/pedigree', methods=['GET'])
@login_required
def serial_pedigree(serie_id):
    """Retorna la historia completa (Pedigree) de un serial."""
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT t.*, u.username as usuario, 
                   c.tipo_comprobante, c.punto_venta, c.numero as comp_nro,
                   ter.nombre as tercero_nombre,
                   dep.nombre as deposito_nombre
            FROM stk_series_trazabilidad t
            JOIN sys_users u ON t.user_id = u.id
            LEFT JOIN erp_comprobantes c ON t.comprobante_id = c.id
            LEFT JOIN erp_terceros ter ON t.tercero_id = ter.id
            LEFT JOIN stk_depositos dep ON t.deposito_id = dep.id
            WHERE t.serie_id = %s AND t.enterprise_id = %s
            ORDER BY t.fecha_efectiva DESC, t.fecha DESC
        """, (serie_id, ent_id))
        history = cursor.fetchall()
    return jsonify(history)


@stock_bp.route('/seriales/<int:articulo_id>', methods=['GET'])
@login_required
@permission_required('view_stock')
def seriales_articulo(articulo_id):
    """Pantalla principal de gestión de series de un artículo."""
    ent_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute(
            "SELECT id, nombre, isbn, requiere_serie FROM stk_articulos WHERE id = %s AND enterprise_id = %s",
            (articulo_id, ent_id)
        )
        articulo = cursor.fetchone()
        if not articulo:
            flash('Artículo no encontrado.', 'danger')
            return redirect(url_for('stock.articulos'))

        cursor.execute(
            "SELECT * FROM stk_numeros_serie WHERE articulo_id = %s AND enterprise_id = %s ORDER BY id DESC LIMIT 200",
            (articulo_id, ent_id)
        )
        series = cursor.fetchall()

        cursor.execute(
            "SELECT ultimo_correlativo, prefijo FROM stk_series_counter WHERE articulo_id = %s AND enterprise_id = %s",
            (articulo_id, ent_id)
        )
        counter = cursor.fetchone() or {'ultimo_correlativo': 0, 'prefijo': None}

        printer = _get_default_printer(cursor, ent_id)

    return render_template(
        'stock/seriales.html',
        articulo=articulo,
        series=series,
        counter=counter,
        printer=printer
    )


# ---- MODO 1: Scan one by one ----
@stock_bp.route('/api/seriales/<int:articulo_id>/scan', methods=['POST'])
@login_required
@permission_required('books_add')
def serial_scan(articulo_id):
    ent_id = g.user['enterprise_id']
    data = request.json
    numero = (data.get('numero_serie') or '').strip()
    lote = data.get('lote', '')
    notas = data.get('notas', '')

    if not numero:
        return jsonify({'success': False, 'message': 'Número de serie vacío.'})

    with get_db_cursor() as cursor:
        try:
            cursor.execute("""
                INSERT INTO stk_numeros_serie (enterprise_id, articulo_id, numero_serie, origen, estado, lote, notas)
                VALUES (%s, %s, %s, 'MANUAL_SCAN', 'EN_STOCK', %s, %s)
            """, (ent_id, articulo_id, numero, lote, notas))
            new_id = cursor.lastrowid
            return jsonify({'success': True, 'id': new_id, 'numero_serie': numero})
        except Exception as e:
            msg = str(e)
            if 'Duplicate' in msg:
                return jsonify({'success': False, 'message': f'Serie duplicada: {numero}'})
            return jsonify({'success': False, 'message': msg})


# ---- MODO 2: Import from CSV/Excel file ----
@stock_bp.route('/api/seriales/<int:articulo_id>/import', methods=['POST'])
@login_required
@permission_required('books_add')
def serial_import(articulo_id):
    ent_id = g.user['enterprise_id']
    file = request.files.get('file')
    column = request.form.get('columna', '0')  # column name or index
    lote = request.form.get('lote', '')

    if not file:
        return jsonify({'success': False, 'message': 'No se recibió archivo.'})

    try:
        content = file.read().decode('utf-8', errors='ignore')
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error leyendo archivo: {e}'})

    # Detect column by name or index
    header = rows[0] if rows else []
    try:
        col_idx = int(column)
    except ValueError:
        # Name-based
        col_idx = next((i for i, h in enumerate(header) if column.lower() in h.lower()), 0)

    inserted = 0
    duplicates = []
    errors = []

    with get_db_cursor() as cursor:
        for row in rows[1:]:  # skip header
            if not row or col_idx >= len(row):
                continue
            numero = row[col_idx].strip()
            if not numero:
                continue
            try:
                cursor.execute("""
                    INSERT IGNORE INTO stk_numeros_serie
                    (enterprise_id, articulo_id, numero_serie, origen, estado, lote)
                    VALUES (%s, %s, %s, 'IMPORTACION', 'EN_STOCK', %s)
                """, (ent_id, articulo_id, numero, lote))
                if cursor.rowcount:
                    inserted += 1
                else:
                    duplicates.append(numero)
            except Exception as e:
                errors.append(str(e))

    return jsonify({
        'success': True,
        'inserted': inserted,
        'duplicates': len(duplicates),
        'errors': len(errors),
        'message': f'{inserted} series importadas, {len(duplicates)} duplicadas ignoradas.'
    })


# ---- MODO 3: Auto-generate correlative ----
@stock_bp.route('/api/seriales/<int:articulo_id>/autogenerar', methods=['POST'])
@login_required
@permission_required('books_add')
def serial_autogenerar(articulo_id):
    ent_id = g.user['enterprise_id']
    data = request.json
    cantidad = int(data.get('cantidad', 1))
    prefijo = data.get('prefijo', '').strip() or None
    imprimir = data.get('imprimir', False)
    lote = data.get('lote', '')

    if cantidad < 1 or cantidad > 500:
        return jsonify({'success': False, 'message': 'Cantidad debe estar entre 1 y 500.'})

    generados = []
    with get_db_cursor() as cursor:
        # Get or create counter
        cursor.execute(
            "SELECT ultimo_correlativo, prefijo FROM stk_series_counter WHERE articulo_id = %s AND enterprise_id = %s",
            (articulo_id, ent_id)
        )
        row = cursor.fetchone()
        start = (row[0] if row else 0)
        pfx = prefijo or (row[1] if row else f'SKU{articulo_id:04d}')

        for i in range(cantidad):
            n = start + i + 1
            # Format: PREFIX-000001 (EAN-friendly, 6 digits)
            serie_num = f"{pfx}-{n:06d}"
            try:
                cursor.execute("""
                    INSERT INTO stk_numeros_serie
                    (enterprise_id, articulo_id, numero_serie, origen, estado, lote)
                    VALUES (%s, %s, %s, 'AUTOGENERADO', 'EN_STOCK', %s)
                """, (ent_id, articulo_id, serie_num, lote))
                generados.append(serie_num)
            except Exception:
                pass  # skip if somehow duplicate

        # Update counter
        nuevo_correlativo = start + len(generados)
        cursor.execute("""
            INSERT INTO stk_series_counter (enterprise_id, articulo_id, ultimo_correlativo, prefijo)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE ultimo_correlativo = %s, prefijo = %s
        """, (ent_id, articulo_id, nuevo_correlativo, pfx, nuevo_correlativo, pfx))

    return jsonify({
        'success': True,
        'generados': generados,
        'total': len(generados),
        'prefijo_usado': pfx,
        'imprimir': imprimir,
        'message': f'{len(generados)} series autogeneradas.'
    })


# ---- CRUD: eliminar una serie ----
@stock_bp.route('/api/seriales/<int:serie_id>/eliminar', methods=['DELETE'])
@login_required
@permission_required('books_add')
def serial_eliminar(serie_id):
    ent_id = g.user['enterprise_id']
    with get_db_cursor() as cursor:
        try:
            cursor.execute(
                "DELETE FROM stk_numeros_serie WHERE id = %s AND enterprise_id = %s",
                (serie_id, ent_id)
            )
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)})


# ---- API: listar series de un artículo ----
@stock_bp.route('/api/seriales/<int:articulo_id>/list', methods=['GET'])
@login_required
def serial_list_api(articulo_id):
    ent_id = g.user['enterprise_id']
    estado = request.args.get('estado', '')
    with get_db_cursor(dictionary=True) as cursor:
        q = "SELECT * FROM stk_numeros_serie WHERE articulo_id = %s AND enterprise_id = %s"
        params = [articulo_id, ent_id]
        if estado:
            q += " AND estado = %s"
            params.append(estado)
        q += " ORDER BY id DESC LIMIT 300"
        cursor.execute(q, params)
        rows = cursor.fetchall()
    return jsonify({'success': True, 'series': rows, 'total': len(rows)})
