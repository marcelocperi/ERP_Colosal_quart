import os

path = 'ventas/routes.py'

with open(path, 'r', encoding='utf-8') as f:
    code = f.read()

new_route = '''
@ventas_bp.route('/api/clientes/<int:id>/cuenta_corriente')
@login_required
@permission_required('ventas', 'admin')
def api_cuenta_corriente(id):
    enterprise_id = g.user['enterprise_id']
    cursor = get_db_cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                c.fecha_emision                              AS fecha,
                c.tipo_comprobante                          AS tipo_doc,
                CONCAT(
                    LPAD(c.punto_venta, 4, '0'), '-',
                    LPAD(c.numero,      8, '0')
                )                                           AS nro_documento,
                NULL                                        AS nro_recibo,
                NULL                                        AS nro_doc_aplicado,
                c.importe_total                             AS importe_bruto,
                c.tipo_comprobante                          AS _signo_tipo,
                c.id                                        AS comprobante_id,
                c.asiento_id                                AS asiento_id,
                COALESCE((
                    SELECT SUM(ci.importe)
                    FROM erp_comprobantes_impuestos ci
                    WHERE ci.comprobante_id = c.id
                      AND ci.enterprise_id  = c.enterprise_id
                ), 0)                                       AS total_percepciones,
                0                                           AS total_retenciones
            FROM erp_comprobantes c
            WHERE c.tercero_id = %s
              AND c.enterprise_id = %s
              AND c.modulo = 'VEN'
        """, (id, enterprise_id))
        rows_comp = cursor.fetchall()

        cursor.execute("""
            SELECT
                r.fecha                                     AS fecha,
                'REC'                                       AS tipo_doc,
                NULL                                        AS nro_documento,
                CONCAT(
                    LPAD(r.punto_venta, 4, '0'), '-',
                    LPAD(r.numero,      8, '0')
                )                                           AS nro_recibo,
                GROUP_CONCAT(
                    DISTINCT CONCAT(
                        LPAD(c.punto_venta, 4, '0'), '-',
                        LPAD(c.numero,      8, '0')
                    )
                    ORDER BY c.numero
                    SEPARATOR ' / '
                )                                           AS nro_doc_aplicado,
                SUM(rd.importe)                             AS importe_bruto,
                'REC'                                       AS _signo_tipo,
                NULL                                        AS comprobante_id,
                r.asiento_id                                AS asiento_id,
                0                                           AS total_percepciones,
                COALESCE((
                    SELECT SUM(re.importe_retencion)
                    FROM fin_retenciones_emitidas re
                    WHERE re.comprobante_pago_id = r.id
                      AND re.enterprise_id = r.enterprise_id
                ), 0)                                       AS total_retenciones
            FROM fin_recibos r
            JOIN fin_recibos_detalles rd ON rd.recibo_id = r.id
            JOIN erp_comprobantes c      ON c.id = rd.factura_id
            WHERE r.tercero_id = %s
              AND r.enterprise_id = %s
            GROUP BY r.id, r.fecha, r.punto_venta, r.numero, r.asiento_id
        """, (id, enterprise_id))
        rows_rec = cursor.fetchall()

        DEBITO_TIPOS = {'001','002','006','007','011','012','005','010','015'}
        NC_TIPOS     = {'003','008','013'}

        cuenta_corriente = []
        saldo = 0.0
        for row in sorted(list(rows_comp) + list(rows_rec), key=lambda r: (r['fecha'] or '1900-01-01')):
            tipo = row['_signo_tipo']
            importe = float(row['importe_bruto'] or 0)

            if tipo in DEBITO_TIPOS:
                debe = importe
                haber = 0.0
                saldo += importe
            elif tipo in NC_TIPOS:
                debe = 0.0
                haber = importe
                saldo -= importe
            else:
                debe = 0.0
                haber = importe
                saldo -= importe

            cuenta_corriente.append({
                'fecha':              row['fecha'],
                'tipo_doc':           row['tipo_doc'],
                'nro_documento':      row['nro_documento'],
                'nro_recibo':         row['nro_recibo'],
                'nro_doc_aplicado':   row['nro_doc_aplicado'],
                'debe':               debe,
                'haber':              haber,
                'saldo':              saldo,
                'comprobante_id':     row['comprobante_id'],
                'asiento_id':         row['asiento_id'],
                'total_percepciones': float(row['total_percepciones']),
                'total_retenciones':  float(row['total_retenciones'])
            })
            
        cursor.close()
        return render_template('ventas/cuenta_corriente_modal.html', cuenta_corriente=cuenta_corriente)

    except Exception as e:
        cursor.close()
        current_app.logger.error(str(e))
        return f"<div class='alert alert-danger'>Error al cargar cuenta corriente</div>", 500
'''

if 'api_cuenta_corriente' not in code:
    code += new_route

with open(path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Route api_cuenta_corriente created")
