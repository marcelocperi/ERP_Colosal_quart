from quart import Blueprint, render_template, request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required, permission_required
from database import get_db_cursor, atomic_transaction
from services.industrial_costing_service import IndustrialCostingService
import datetime

produccion_bp = Blueprint('produccion', __name__, template_folder='templates')

@produccion_bp.route('/produccion/dashboard')
@login_required
@permission_required('produccion.dashboard')
async def dashboard():
    """Tablero Principal de Producción."""
    try:
        return await render_template('produccion/dashboard.html')
    except Exception as e:
        import traceback
        traceback.print_exc()
        await flash(f"Error al cargar el dashboard de producción: {str(e)}", "danger")
        return redirect('/')

@produccion_bp.route('/produccion/overhead-templates', methods=['GET'])
@login_required
@permission_required('produccion.admin')
async def overhead_templates():
    """Listado de Plantillas de Costos Indirectos."""
    ent_id = g.user['enterprise_id']
    templates = []
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute('''
            SELECT id, nombre, descripcion, activo, created_at 
            FROM cmp_overhead_templates 
            WHERE enterprise_id = %s
            ORDER BY nombre ASC
        ''', (ent_id,))
        templates = await cursor.fetchall()
        
        for t in templates:
            await cursor.execute('''
                SELECT 
                    COUNT(*) as qty_items, 
                    SUM(monto_estimado) as total_estimado 
                FROM cmp_overhead_templates_detalle 
                WHERE template_id = %s
            ''', (t['id'],))
            stats = await cursor.fetchone()
            t['detalles_count'] = stats['qty_items'] or 0
            t['suma_estimada'] = stats['total_estimado'] or 0.0

    return await render_template('produccion/overhead_templates.html', templates=templates)

@produccion_bp.route('/produccion/overhead-templates/api/save', methods=['POST'])
@login_required
@permission_required('produccion.admin')
async def api_save_overhead_template():
    ent_id = g.user['enterprise_id']
    data = (await request.json)
    nombre = data.get('nombre')
    descripcion = data.get('descripcion', '')
    detalles = data.get('detalles', [])
    
    if not nombre:
        return jsonify({'success': False, 'message': 'El nombre es obligatorio'}), 400
        
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute('''
                INSERT INTO cmp_overhead_templates 
                (enterprise_id, nombre, descripcion, user_id)
                VALUES (%s, %s, %s, %s)
            ''', (ent_id, nombre, descripcion, g.user['id']))
            template_id = cursor.lastrowid
            
            for det in detalles:
                await cursor.execute('''
                    INSERT INTO cmp_overhead_templates_detalle
                    (template_id, enterprise_id, tipo_gasto, descripcion, monto_estimado, base_calculo, cantidad_batch, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (template_id, ent_id, det['tipo_gasto'], det['descripcion'], det['monto_estimado'], det['base_calculo'], det.get('cantidad_batch', 1), g.user['id']))
        
        return jsonify({'success': True, 'message': 'Plantilla guardada exitosamente.', 'template_id': template_id})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error guardando plantilla: {str(e)}'}), 500

@produccion_bp.route('/produccion/overhead-templates/<int:template_id>/api/detalles', methods=['GET'])
@login_required
async def api_get_overhead_details(template_id):
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute('''
            SELECT tipo_gasto, descripcion, monto_estimado, base_calculo, cantidad_batch
            FROM cmp_overhead_templates_detalle
            WHERE template_id = %s AND enterprise_id = %s
        ''', (template_id, ent_id))
        detalles = await cursor.fetchall()
    return jsonify({'success': True, 'detalles': detalles})

@produccion_bp.route('/produccion/documentos', methods=['GET'])
@login_required
@permission_required('produccion.view')
async def documentos():
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute('''
            SELECT sys_documentos_adjuntos.*, 
                CASE 
                    WHEN entidad_tipo = 'ARTICULO' THEN (SELECT nombre FROM stk_articulos WHERE id = sys_documentos_adjuntos.entidad_id)
                    WHEN entidad_tipo = 'PROVEEDOR' THEN (SELECT nombre FROM erp_terceros WHERE id = sys_documentos_adjuntos.entidad_id)
                    ELSE 'N/A' 
                END as entidad_nombre
            FROM sys_documentos_adjuntos
            WHERE sys_documentos_adjuntos.enterprise_id = %s
            ORDER BY sys_documentos_adjuntos.fecha_vencimiento ASC
        ''', (ent_id,))
        documentos = await cursor.fetchall()
    return await render_template('produccion/documentos.html', documentos=documentos)

@produccion_bp.route('/produccion/proyectos', methods=['GET'])
@login_required
@permission_required('produccion.admin')
async def proyectos():
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute('''
            SELECT prd_proyectos_desarrollo.*, stk_articulos.nombre as producto_nombre
            FROM prd_proyectos_desarrollo
            LEFT JOIN stk_articulos ON prd_proyectos_desarrollo.articulo_objetivo_id = stk_articulos.id
            WHERE prd_proyectos_desarrollo.enterprise_id = %s
            ORDER BY prd_proyectos_desarrollo.fecha_inicio DESC
        ''', (ent_id,))
        proyectos = await cursor.fetchall()
    return await render_template('produccion/proyectos.html', proyectos=proyectos)

@produccion_bp.route('/produccion/bandeja-costos')
@login_required
@permission_required('cost_accounting')
async def bandeja_costos():
    """Bandeja Global de Costos (antes en Pricing)."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute('''
            SELECT stk_pricing_propuestas.*, stk_articulos.nombre as articulo_nombre, stk_articulos.codigo as articulo_codigo, 
                   stk_metodos_costeo.nombre as metodo_nombre,
                    stk_listas_precios.nombre as lista_nombre
            FROM stk_pricing_propuestas
            JOIN stk_articulos ON stk_pricing_propuestas.articulo_id = stk_articulos.id
            LEFT JOIN stk_metodos_costeo ON stk_pricing_propuestas.metodo_costeo_id = stk_metodos_costeo.id
            LEFT JOIN stk_listas_precios ON stk_pricing_propuestas.lista_id = stk_listas_precios.id
            WHERE stk_pricing_propuestas.estado = 'PENDIENTE' AND stk_pricing_propuestas.enterprise_id = %s
            ORDER BY stk_pricing_propuestas.fecha_propuesta DESC
        ''', (g.user['enterprise_id'],))
        propuestas = await cursor.fetchall()
    return await render_template('produccion/bandeja_costos.html', propuestas=propuestas)

@produccion_bp.route('/industrial/api/costeo/<int:propuesta_id>/aprobar', methods=['POST'])
@login_required
@permission_required('cost_accounting')
async def api_aprobar_costeo(propuesta_id):
    ent_id = g.user['enterprise_id']
    try:
        async with get_db_cursor() as cursor:
            # 1. Obtener datos de la propuesta
            await cursor.execute("SELECT articulo_id, costo_propuesto FROM stk_pricing_propuestas WHERE id = %s AND enterprise_id = %s AND estado = 'PENDIENTE'", (propuesta_id, ent_id))
            p = await cursor.fetchone()
            if not p: return jsonify({'success': False, 'message': 'Propuesta no encontrada o ya procesada.'}), 404
            articulo_id, costo_propuesto = p
            
            # 2. Aprobar
            await cursor.execute("UPDATE stk_pricing_propuestas SET estado = 'APROBADO', user_id_update = %s, updated_at = NOW() WHERE id = %s", (g.user['id'], propuesta_id))
            
            # 3. Impactar en el artículo (costo_reposicion)
            await cursor.execute("UPDATE stk_articulos SET costo_reposicion = %s, costo_ultima_compra = %s WHERE id = %s AND enterprise_id = %s", (costo_propuesto, costo_propuesto, articulo_id, ent_id))
            
        return jsonify({'success': True, 'message': 'Costeo impactado exitosamente.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@produccion_bp.route('/industrial/api/costeo/<int:propuesta_id>/rechazar', methods=['POST'])
@login_required
@permission_required('cost_accounting')
async def api_rechazar_costeo(propuesta_id):
    ent_id = g.user['enterprise_id']
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("UPDATE stk_pricing_propuestas SET estado = 'RECHAZADO', user_id_update = %s, updated_at = NOW() WHERE id = %s AND enterprise_id = %s", (g.user['id'], propuesta_id, ent_id))
        return jsonify({'success': True, 'message': 'Costeo rechazado.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
