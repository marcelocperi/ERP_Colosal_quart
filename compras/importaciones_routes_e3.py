# ==============================================================================
# MÓDULO DE IMPORTACIONES — ETAPA 3
# Finanzas y Pagos Internacionales (SWIFT, Transferencias, Bancos)
# ==============================================================================

from quart import request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required
from database import get_db_cursor, atomic_transaction
from services.importacion_service import ImportacionService

def register_importaciones_routes_e3(bp):
    """Etapa 3: Gestión de Pagos Internacionales."""

    @bp.route('/compras/importaciones/orden/<int:orden_id>/pago/agregar', methods=['POST'])
    @login_required
    @atomic_transaction('compras')
    async def importacion_agregar_pago(orden_id):
        """Registra un pago internacional vinculado a una orden."""
        ent_id = g.user['enterprise_id']
        
        monto_orig   = float((await request.form).get('monto_orig', 0) or 0)
        moneda       = (await request.form).get('moneda', 'USD').upper()
        tipo_cambio  = float((await request.form).get('tipo_cambio', 0) or 0)
        banco_id     = (await request.form).get('banco_id')
        fecha        = (await request.form).get('fecha')
        swift        = (await request.form).get('referencia_swift', '')
        observaciones = (await request.form).get('observaciones', '')
        proveedor_id = (await request.form).get('proveedor_id')

        if not banco_id or monto_orig <= 0 or tipo_cambio <= 0:
            await flash("Datos de pago incompletos o inválidos.", "danger")
            return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

        try:
            resultado = await ImportacionService.agregar_pago(
                enterprise_id=ent_id,
                orden_id=orden_id,
                proveedor_id=proveedor_id,
                monto_orig=monto_orig,
                moneda=moneda,
                tipo_cambio=tipo_cambio,
                banco_id=banco_id,
                fecha=fecha,
                swift=swift,
                observaciones=observaciones,
                user_id=g.user['id']
            )
            
            if resultado['success']:
                await flash(f"✅ Pago de {moneda} {monto_orig:,.2f} registrado. Asiento: {resultado['asiento_id']}", "success")
            else:
                await flash("Error al registrar el pago.", "danger")
                
        except Exception as e:
            await flash(f"Error: {str(e)}", "danger")

        return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

    @bp.route('/compras/api/importaciones/orden/<int:orden_id>/pagos')
    @login_required
    async def api_get_pagos_orden(orden_id):
        """API para obtener los pagos realizados."""
        ent_id = g.user['enterprise_id']
        try:
            pagos = await ImportacionService.get_pagos_orden(orden_id, ent_id)
            # Serializar fechas para JSON
            for p in pagos:
                if hasattr(p.get('fecha'), 'isoformat'):
                    p['fecha'] = p['fecha'].isoformat()
                if hasattr(p.get('created_at'), 'isoformat'):
                    p['created_at'] = p['created_at'].isoformat()
            
            return jsonify({'success': True, 'data': pagos})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
