
from quart import render_template, request, jsonify, g, flash, redirect, url_for
from core.decorators import login_required, permission_required
from database import get_db_cursor
from services.consignment_service import ConsignmentService

def register_fazon_routes(bp):
    """
    Rutas para Phase 1.5: Consignaciones y Fazón (Stock de Terceros).
    Maneja:
    - Depósitos de terceros (Taller, Consignatario).
    - Liquidaciones de consumo.
    - Control de stock externo.
    """

    @bp.route('/compras/fazon/dashboard', methods=['GET'])
    @login_required
    @permission_required('view_compras')
    async def fazon_dashboard():
        """Tablero de Control de Stock Externo (Fazón/Consignación)."""
        ent_id = g.user['enterprise_id']
        async with get_db_cursor(dictionary=True) as cursor:
            # 1. Traer depósitos de tipo FAZON o CONSIGNACION
            await cursor.execute('''
                SELECT d.*, t.nombre as tercero_nombre 
                FROM stk_depositos d
                LEFT JOIN erp_terceros t ON d.tercero_id = t.id
                WHERE d.enterprise_id = %s AND d.tipo_propiedad != 'PROPIO'
                ORDER BY d.nombre ASC
            ''', (ent_id,))
            depositos_externos = await cursor.fetchall()
            
            # 2. Resumen de stock en consignación
            stock_consignado = await ConsignmentService.get_stock_en_consignacion(ent_id)
            
            # 3. Liquidaciones pendientes de facturar
            await cursor.execute('''
                SELECT l.*, t.nombre as tercero_nombre, d.nombre as deposito_nombre
                FROM stk_liquidaciones_consignacion l
                JOIN erp_terceros t ON l.tercero_id = t.id
                JOIN stk_depositos d ON l.deposito_id = d.id
                WHERE l.enterprise_id = %s AND l.estado = 'PENDIENTE_FACTURACION'
                ORDER BY l.fecha_reporte DESC
            ''', (ent_id,))
            liquidaciones_pendientes = await cursor.fetchall()

        return await render_template('compras/fazon/dashboard.html', 
                             depositos=depositos_externos, 
                             stock=stock_consignado, 
                             liquidaciones=liquidaciones_pendientes)

    @bp.route('/compras/fazon/api/reportar-consumo', methods=['POST'])
    @login_required
    @permission_required('compras.facturar_compras')
    async def api_fazon_reportar_consumo():
        """Registra el consumo reportado por el tercero y genera la liquidación."""
        data = (await request.json)
        ent_id = g.user['enterprise_id']
        uid = g.user['id']
        
        tercero_id = data.get('tercero_id')
        deposito_id = data.get('deposito_id')
        items = data.get('items', []) # [{articulo_id, cantidad_consumida, precio_pactado}, ...]
        
        if not tercero_id or not deposito_id or not items:
            return jsonify({'success': False, 'message': 'Datos incompletos'}), 400
            
        try:
            async with get_db_cursor() as cursor:
                # 1. Crear liquidación cabecera
                await cursor.execute('''
                    INSERT INTO stk_liquidaciones_consignacion 
                    (enterprise_id, tercero_id, deposito_id, estado, user_id)
                    VALUES (%s, %s, %s, 'PENDIENTE_FACTURACION', %s)
                ''', (ent_id, tercero_id, deposito_id, uid))
                liq_id = cursor.lastrowid
                
                # 2. Insertar detalles
                for it in items:
                    await cursor.execute('''
                        INSERT INTO stk_liquidaciones_consignacion_det 
                        (liquidacion_id, articulo_id, cantidad_consumida, precio_pactado)
                        VALUES (%s, %s, %s, %s)
                    ''', (liq_id, it['articulo_id'], it['cantidad_consumida'], it['precio_pactado']))
                    
                    # 3. Descontar stock del depósito externo
                    # Nota: Aquí se debería invocar al StockService para registrar el egreso por consumo
                    await cursor.execute('''
                        UPDATE stk_existencias 
                        SET cantidad = cantidad - %s 
                        WHERE enterprise_id = %s AND deposito_id = %s AND articulo_id = %s
                    ''', (it['cantidad_consumida'], ent_id, deposito_id, it['articulo_id']))
                
            return jsonify({'success': True, 'liquidacion_id': liq_id, 'message': 'Reporte de consumo procesado exitosamente.'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
