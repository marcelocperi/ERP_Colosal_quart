from quart import Blueprint, render_template, request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required, permission_required
from pricing.service import PricingService
import logging
from database import get_db_cursor, atomic_transaction
import datetime

pricing_bp = Blueprint('pricing', __name__, template_folder='templates')

@pricing_bp.route('/pricing/dashboard')
@login_required
async def dashboard():
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # Get price lists with pending count
            await cursor.execute("""
                SELECT stk_listas_precios.*, 
                       (SELECT COUNT(*) FROM stk_pricing_propuestas 
                        WHERE stk_pricing_propuestas.lista_id = stk_listas_precios.id AND stk_pricing_propuestas.estado = 'PENDIENTE' AND stk_pricing_propuestas.enterprise_id = %s) as pending_count
                FROM stk_listas_precios 
                WHERE stk_listas_precios.enterprise_id = %s OR stk_listas_precios.enterprise_id = 0
            """, (g.user['enterprise_id'], g.user['enterprise_id']))
            listas = await cursor.fetchall()
            
            # Get recent price updates
            await cursor.execute("""
                SELECT stk_articulos_precios.*, stk_articulos.nombre as articulo_nombre, stk_listas_precios.nombre as lista_nombre
                FROM stk_articulos_precios
                JOIN stk_articulos ON stk_articulos_precios.articulo_id = stk_articulos.id
                JOIN stk_listas_precios ON stk_articulos_precios.lista_precio_id = stk_listas_precios.id
                WHERE stk_articulos_precios.enterprise_id = %s
                ORDER BY stk_articulos_precios.fecha_inicio_vigencia DESC LIMIT 10
            """, (g.user['enterprise_id'],))
            ultimos_precios = await cursor.fetchall()
    
        return await render_template('pricing/dashboard.html', listas=listas, ultimos_precios=ultimos_precios)
    except Exception as e:
        import traceback
        traceback.print_exc()
        await flash(f"Error al cargar el dashboard de pricing: {str(e)}", "danger")
        return redirect('/')

@pricing_bp.route('/pricing/lista/<int:lista_id>')
@login_required
async def lista_detalle(lista_id):
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM stk_listas_precios WHERE id = %s AND (enterprise_id = %s OR enterprise_id = 0)", (lista_id, g.user['enterprise_id']))
        lista = await cursor.fetchone()
        if not lista:
            await flash("Lista de precios no encontrada.", "danger")
            return redirect(url_for('pricing.dashboard'))

        # Get rules for this list
        await cursor.execute("""
            SELECT stk_pricing_reglas.*, stk_metodos_costeo.nombre as metodo_nombre
            FROM stk_pricing_reglas
            JOIN stk_metodos_costeo ON stk_pricing_reglas.metodo_costo_id = stk_metodos_costeo.id
            WHERE stk_pricing_reglas.lista_precio_id = %s AND stk_pricing_reglas.enterprise_id = %s
            ORDER BY stk_pricing_reglas.prioridad DESC
        """, (lista_id, g.user['enterprise_id']))
        reglas = await cursor.fetchall()
        
        # Get methods for selection
        await cursor.execute("SELECT * FROM stk_metodos_costeo")
        metodos = await cursor.fetchall()
        
        # Get natures for selection
        naturalezas = ['PRODUCTO', 'SERVICIO', 'LIBRO', 'ABONO', 'COMBO']

        # Count pending proposals to control Recalculate button
        await cursor.execute("""
            SELECT COUNT(*) FROM stk_pricing_propuestas 
            WHERE lista_id = %s AND estado = 'PENDIENTE' AND enterprise_id = %s
        """, (lista_id, g.user['enterprise_id']))
        pending_count = await cursor.fetchone()[0]

    return await render_template('pricing/lista_detalle.html', lista=lista, reglas=reglas, metodos=metodos, naturalezas=naturalezas, pending_count=pending_count)

@pricing_bp.route('/reglas/guardar', methods=['POST'])
@login_required
async def regla_guardar():
    try:
        rid = (await request.form).get('id')
        lista_id = (await request.form).get('lista_id')
        naturaleza = (await request.form).get('naturaleza')
        metodo_id = (await request.form).get('metodo_id')
        markup = (await request.form).get('markup')
        prioridad = (await request.form).get('prioridad', 0)
        
        async with get_db_cursor() as cursor:
            if rid:
                await cursor.execute("""
                    UPDATE stk_pricing_reglas 
                    SET naturaleza=%s, metodo_costo_id=%s, coeficiente_markup=%s, prioridad=%s
                    WHERE id=%s AND enterprise_id=%s
                """, (naturaleza, metodo_id, markup, prioridad, rid, g.user['enterprise_id']))
                await flash("Regla actualizada", "success")
            else:
                await cursor.execute("""
                    INSERT INTO stk_pricing_reglas (enterprise_id, lista_precio_id, naturaleza, metodo_costo_id, coeficiente_markup, prioridad)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (g.user['enterprise_id'], lista_id, naturaleza, metodo_id, markup, prioridad))
                await flash("Regla creada", "success")
    except Exception as e:
        await flash(f"Error: {e}", "danger")
    return redirect(url_for('pricing.lista_detalle', lista_id=lista_id))

@pricing_bp.route('/lista/<int:id>/recalcular', methods=['POST'])
@login_required
async def lista_recalcular(id):
    try:
        count = await PricingService.calculate_list_prices(g.user['enterprise_id'], id, g.user['id'])
        await flash(f"Se han generado {count} propuestas de precio. Esperando aprobación de Cost Accounting.", "info")
    except Exception as e:
        await flash(f"Error al generar propuestas: {e}", "danger")
    return redirect(url_for('pricing.lista_detalle', lista_id=id))

@pricing_bp.route('/lista/<int:id>/pendientes')
@login_required
@permission_required('view_precios') # Temporary until 'cost_accounting' is seeded
async def lista_pendientes(id):
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT id, nombre FROM stk_listas_precios WHERE id = %s", (id,))
        lista = await cursor.fetchone()
        
        await cursor.execute("""
            SELECT stk_pricing_propuestas.*, stk_articulos.nombre as articulo_nombre, stk_articulos.codigo as articulo_codigo, stk_metodos_costeo.nombre as metodo_nombre
            FROM stk_pricing_propuestas
            JOIN stk_articulos ON stk_pricing_propuestas.articulo_id = stk_articulos.id
            LEFT JOIN stk_metodos_costeo ON stk_pricing_propuestas.metodo_costeo_id = stk_metodos_costeo.id
            WHERE stk_pricing_propuestas.lista_id = %s AND stk_pricing_propuestas.estado = 'PENDIENTE' AND stk_pricing_propuestas.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        propuestas = await cursor.fetchall()
        
    return await render_template('pricing/lista_pendientes.html', lista=lista, propuestas=propuestas)

@pricing_bp.route('/propuestas/accion', methods=['POST'])
@login_required
@permission_required('view_precios') # Temporary until 'cost_accounting' is seeded
async def propuesta_accion():
    try:
        propuesta_ids = (await request.form).getlist('propuesta_ids')
        accion = (await request.form).get('accion') # 'APROBADO' o 'RECHAZADO'
        motivo = (await request.form).get('motivo', '')
        lista_id = (await request.form).get('lista_id')
        
        if not propuesta_ids:
            await flash("Debe seleccionar al menos un artículo.", "warning")
            return redirect(url_for('pricing.todas_las_pendientes') if not lista_id else url_for('pricing.lista_pendientes', id=lista_id))
            
        count = await PricingService.procesar_aprobacion(g.user['enterprise_id'], propuesta_ids, accion, motivo, g.user['id'])
        await flash(f"{count} propuestas procesadas con éxito ({accion}).", "success")
    except Exception as e:
        await flash(f"Error al procesar: {e}", "danger")
    
    if (await request.form).get('from_global') == '1':
        return redirect(url_for('pricing.todas_las_pendientes'))
    
    return redirect(url_for('pricing.lista_pendientes', id=lista_id) if lista_id else url_for('pricing.todas_las_pendientes'))

@pricing_bp.route('/pricing/todas_las_pendientes')
@login_required
@permission_required('cost_accounting')
async def todas_las_pendientes():
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT stk_pricing_propuestas.*, stk_articulos.nombre as articulo_nombre, stk_articulos.codigo as articulo_codigo, 
                   stk_metodos_costeo.nombre as metodo_nombre,
                   stk_listas_precios.nombre as lista_nombre
            FROM stk_pricing_propuestas
            JOIN stk_articulos ON stk_pricing_propuestas.articulo_id = stk_articulos.id
            LEFT JOIN stk_metodos_costeo ON stk_pricing_propuestas.metodo_costeo_id = stk_metodos_costeo.id
            LEFT JOIN stk_listas_precios ON stk_pricing_propuestas.lista_id = stk_listas_precios.id
            WHERE stk_pricing_propuestas.estado = 'PENDIENTE' AND stk_pricing_propuestas.enterprise_id = %s
            ORDER BY stk_pricing_propuestas.fecha_propuesta DESC
        """, (g.user['enterprise_id'],))
        propuestas = await cursor.fetchall()
        
    return await render_template('pricing/pendientes_globales.html', propuestas=propuestas)
