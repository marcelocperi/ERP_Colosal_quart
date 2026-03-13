import os
from quart import render_template, request, jsonify, g, flash, redirect, url_for
from core.decorators import login_required, permission_required
from database import get_db_cursor
from services.industrial_costing_service import IndustrialCostingService

def register_industrial_routes(bp):
    """
    Rutas para el Módulo de Ingeniería y Costos Industriales (MSAC v4.1).
    Maneja:
    - Plantillas de Costos Indirectos (Overhead: Mano de Obra, Energía, Calidad, etc.)
    - Repositorio Documental Técnico/Legal (Próximamente)
    - Proyectos de Desarrollo I+D (Próximamente)
    """

    @bp.route('/industrial/overhead-templates', methods=['GET'])
    @login_required
    async def overhead_templates():
        """Listado de Plantillas de Costos Indirectos."""
        ent_id = g.user['enterprise_id']
        templates = []
        async with get_db_cursor(dictionary=True) as cursor:
            # Traer plantillas
            await cursor.execute('''
                SELECT id, nombre, descripcion, activo, created_at 
                FROM cmp_overhead_templates 
                WHERE enterprise_id = %s
                ORDER BY nombre ASC
            ''', (ent_id,))
            templates = await cursor.fetchall()
            
            # Traer detalles agregados (cantidad de items y suma estimada)
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

        return await render_template('compras/industrial/overhead_templates.html', templates=templates)

    @bp.route('/industrial/overhead-templates/api/save', methods=['POST'])
    @login_required
    async def api_save_overhead_template():
        """Crea o actualiza una plantilla de costos indirectos (AJAX)."""
        ent_id = g.user['enterprise_id']
        data = (await request.json)
        
        nombre = data.get('nombre')
        descripcion = data.get('descripcion', '')
        detalles = data.get('detalles', [])
        
        if not nombre:
            return jsonify({'success': False, 'message': 'El nombre es obligatorio'}), 400
            
        try:
            async with get_db_cursor() as cursor:
                # 1. Crear el Template base
                await cursor.execute('''
                    INSERT INTO cmp_overhead_templates 
                    (enterprise_id, nombre, descripcion, user_id)
                    VALUES (%s, %s, %s, %s)
                ''', (ent_id, nombre, descripcion, g.user['id']))
                template_id = cursor.lastrowid
                
                # 2. Insertar cada renglón de gasto (MOD, Energia, Ensayos, etc.)
                for det in detalles:
                    await cursor.execute('''
                        INSERT INTO cmp_overhead_templates_detalle
                        (template_id, enterprise_id, tipo_gasto, descripcion, monto_estimado, base_calculo, cantidad_batch, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        template_id, ent_id, 
                        det['tipo_gasto'], 
                        det['descripcion'], 
                        det['monto_estimado'], 
                        det['base_calculo'], 
                        det.get('cantidad_batch', 1),
                        g.user['id']
                    ))
            
            return jsonify({'success': True, 'message': 'Plantilla guardada exitosamente.', 'template_id': template_id})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error guardando plantilla: {str(e)}'}), 500

    @bp.route('/industrial/overhead-templates/<int:template_id>/api/detalles', methods=['GET'])
    @login_required
    async def api_get_template_detalles(template_id):
        """Devuelve los detalles de una plantilla específica para aplicarlos a un artículo."""
        ent_id = g.user['enterprise_id']
        async with get_db_cursor(dictionary=True) as cursor:
            # Validar que pertenece a la empresa
            await cursor.execute('SELECT id FROM cmp_overhead_templates WHERE id=%s AND enterprise_id=%s', (template_id, ent_id))
            if not await cursor.fetchone():
                return jsonify({'success': False, 'message': 'No encontrado'}), 404
                
            await cursor.execute('''
                SELECT id, tipo_gasto, descripcion, monto_estimado, base_calculo, cantidad_batch
                FROM cmp_overhead_templates_detalle
                WHERE template_id = %s
            ''', (template_id,))
            detalles = await cursor.fetchall()
            
        return jsonify({'success': True, 'detalles': detalles})

    @bp.route('/industrial/documentos', methods=['GET'])
    @login_required
    async def industrial_documentos():
        """Repositorio Técnico/Legal."""
        ent_id = g.user['enterprise_id']
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute('''
                SELECT d.*, 
                    CASE 
                        WHEN entidad_tipo = 'ARTICULO' THEN (SELECT nombre FROM stk_articulos WHERE id = d.entidad_id)
                        WHEN entidad_tipo = 'PROVEEDOR' THEN (SELECT nombre FROM erp_terceros WHERE id = d.entidad_id)
                        ELSE 'N/A' 
                    END as entidad_nombre
                FROM sys_documentos_adjuntos d
                WHERE d.enterprise_id = %s
                ORDER BY d.fecha_vencimiento ASC
            ''', (ent_id,))
            documentos = await cursor.fetchall()
        return await render_template('compras/industrial/documentos.html', documentos=documentos)

    @bp.route('/industrial/proyectos', methods=['GET'])
    @login_required
    async def industrial_proyectos():
        """Proyectos de I+D."""
        ent_id = g.user['enterprise_id']
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute('''
                SELECT p.*, a.nombre as producto_nombre
                FROM prd_proyectos_desarrollo p
                LEFT JOIN stk_articulos a ON p.articulo_objetivo_id = a.id
                WHERE p.enterprise_id = %s
                ORDER BY p.fecha_inicio DESC
            ''', (ent_id,))
            proyectos = await cursor.fetchall()
        return await render_template('compras/industrial/proyectos.html', proyectos=proyectos)

