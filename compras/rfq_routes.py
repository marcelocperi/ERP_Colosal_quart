
from quart import render_template, request, jsonify, g, flash, redirect, url_for
from core.decorators import login_required, permission_required
from database import get_db_cursor
from services.rfq_service import RfqService

def register_rfq_routes(bp):
    """
    Rutas para Phase 1.4: RFQ Enrichment & Campañas de Sourcing.
    """

    @bp.route('/compras/rfq/campanas', methods=['GET'])
    @login_required
    @permission_required('compras.solicitar_cotizacion')
    async def rfq_campanas():
        """Listado de Campañas de RFQ (Sourcing Masivo)."""
        ent_id = g.user['enterprise_id']
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute('''
                SELECT c.*, a.nombre as producto_nombre,
                       (SELECT COUNT(*) FROM cmp_rfq_detalles WHERE rfq_id = c.id) as qty_items
                FROM cmp_rfq_campanas c
                LEFT JOIN stk_articulos a ON c.articulo_objetivo_id = a.id
                WHERE c.enterprise_id = %s
                ORDER BY c.fecha_emision DESC
            ''', (ent_id,))
            campanas = await cursor.fetchall()
        return await render_template('compras/rfq/campanas_lista.html', campanas=campanas)

    @bp.route('/compras/rfq/api/explode', methods=['POST'])
    @login_required
    async def api_rfq_explode():
        """Explota BOM y devuelve sugerencias de compra para una cantidad objetivo."""
        data = (await request.json)
        producto_id = data.get('producto_id')
        cantidad = float(data.get('cantidad', 1))
        ent_id = g.user['enterprise_id']
        
        if not producto_id:
            return jsonify({'success': False, 'message': 'ID de producto requerido'}), 400
            
        try:
            items = await RfqService.explode_bom_for_rfq(ent_id, producto_id, cantidad)
            return jsonify({'success': True, 'items': items})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    @bp.route('/compras/rfq/api/crear-campana', methods=['POST'])
    @login_required
    @permission_required('compras.solicitar_cotizacion')
    async def api_rfq_crear_campana():
        """Persiste una nueva campaña de RFQ a partir de la explosión."""
        data = (await request.json)
        ent_id = g.user['enterprise_id']
        uid = g.user['id']
        
        articulo_objetivo_id = data.get('producto_id')
        cantidad_objetivo = data.get('cantidad')
        fecha_cierre = data.get('fecha_cierre')
        detalles = data.get('detalles', []) # [{articulo_id, cantidad}, ...]
        
        if not detalles or not fecha_cierre:
            return jsonify({'success': False, 'message': 'Datos incompletos'}), 400
            
        try:
            async with get_db_cursor() as cursor:
                # 1. Cabecera
                await cursor.execute('''
                    INSERT INTO cmp_rfq_campanas 
                    (enterprise_id, fecha_cierre, estado, articulo_objetivo_id, cantidad_objetivo, user_id)
                    VALUES (%s, %s, 'BORRADOR', %s, %s, %s)
                ''', (ent_id, fecha_cierre, articulo_objetivo_id, cantidad_objetivo, uid))
                rfq_id = cursor.lastrowid
                
                # 2. Detalles
                for det in detalles:
                    await cursor.execute('''
                        INSERT INTO cmp_rfq_detalles (rfq_id, articulo_insumo_id, cantidad_requerida, sugerencia_origen)
                        VALUES (%s, %s, %s, 'EXPLOSION_BOM')
                    ''', (rfq_id, det['articulo_id'], det['cantidad']))
                
            return jsonify({'success': True, 'rfq_id': rfq_id, 'message': 'Campaña creada satisfactoriamente.'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
