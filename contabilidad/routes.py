from flask import render_template, request, g, jsonify, flash, redirect, url_for
from contabilidad import contabilidad_bp
from core.decorators import login_required, permission_required
from database import get_db_cursor, atomic_transaction
import datetime
import re
import json
from services.validation_service import format_cuit

@contabilidad_bp.route('/contabilidad/dashboard')
@login_required
def dashboard():
    try:
        return render_template('contabilidad/dashboard.html')
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f"Error al cargar el dashboard de contabilidad: {str(e)}", "danger")
        return redirect('/')

@contabilidad_bp.route('/contabilidad/config-fiscal', methods=['GET', 'POST'])
@login_required
def config_fiscal():
    from services.afip_service import AfipService
    enterprise_id = g.user['enterprise_id']
    
    status = None
    with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            cuit = request.form.get('cuit')
            afip_crt = request.form.get('afip_crt')
            afip_key = request.form.get('afip_key')
            afip_entorno = request.form.get('afip_entorno')
            
            cuit = format_cuit(cuit)
            
            cursor.execute("""
                UPDATE sys_enterprises 
                SET cuit = %s, afip_crt = %s, afip_key = %s, afip_entorno = %s 
                WHERE id = %s
            """, (cuit, afip_crt, afip_key, afip_entorno, enterprise_id))
            flash("Configuración fiscal actualizada correctamente", "success")
            return redirect(url_for('contabilidad.config_fiscal'))

        cursor.execute("SELECT cuit, afip_crt, afip_key, afip_entorno FROM sys_enterprises WHERE id = %s", (enterprise_id,))
        enterprise = cursor.fetchone()
        
    status = AfipService.verificar_configuracion(enterprise_id)
        
    return render_template('contabilidad/config_fiscal.html', 
                          enterprise=enterprise, 
                          status=status)

@contabilidad_bp.route('/contabilidad/libro-iva-ventas', methods=['GET'])
@login_required
def libro_iva_ventas():
    # Parametros de filtro
    today = datetime.date.today()
    anio = request.args.get('anio', today.year, type=int)
    mes = request.args.get('mes', today.month, type=int)
    
    with get_db_cursor(dictionary=True) as cursor:
        # Consulta principal: Comprobantes del periodo
        # La tabla erp_comprobantes tiene la informacion necesaria
        cursor.execute("""
            SELECT 
                erp_comprobantes.id, erp_comprobantes.fecha_emision, erp_comprobantes.tipo_comprobante, erp_comprobantes.punto_venta, erp_comprobantes.numero,
                erp_comprobantes.importe_neto, erp_comprobantes.importe_iva, erp_comprobantes.importe_total,
                erp_terceros.nombre as cliente_nombre, erp_terceros.cuit as cliente_cuit, erp_terceros.tipo_responsable,
                sys_tipos_comprobante.descripcion as tipo_desc, sys_tipos_comprobante.letra
            FROM erp_comprobantes
            JOIN erp_terceros ON erp_comprobantes.tercero_id = erp_terceros.id
            JOIN sys_tipos_comprobante ON erp_comprobantes.tipo_comprobante = sys_tipos_comprobante.codigo
            WHERE erp_comprobantes.enterprise_id = %s 
              AND erp_comprobantes.tipo_operacion = 'VENTA'
              AND YEAR(erp_comprobantes.fecha_emision) = %s 
              AND MONTH(erp_comprobantes.fecha_emision) = %s
            ORDER BY erp_comprobantes.fecha_emision ASC, erp_comprobantes.numero ASC
        """, (g.user['enterprise_id'], anio, mes))
        
        comprobantes_db = cursor.fetchall()
        
        # Procesar datos y calcular totales
        reporte = []
        totales = {'neto': 0.0, 'iva': 0.0, 'total': 0.0}
        
        for comp in comprobantes_db:
            # Detectar si es NC para restar valores en el libro
            es_nc = comp['tipo_comprobante'] in ['003', '008', '013']
            signo = -1 if es_nc else 1
            
            neto = float(comp['importe_neto'] or 0) * signo
            iva = float(comp['importe_iva'] or 0) * signo
            total = float(comp['importe_total'] or 0) * signo
            
            row = {
                'fecha': comp['fecha_emision'].strftime('%d/%m/%Y') if hasattr(comp['fecha_emision'], 'strftime') else comp['fecha_emision'],
                'tipo': f"{comp['tipo_desc']} ({comp['letra']})",
                'numero': f"{comp['punto_venta']:04d}-{comp['numero']:08d}",
                'cliente': comp['cliente_nombre'],
                'cuit': comp['cliente_cuit'],
                'condicion': comp['tipo_responsable'],
                'neto': neto,
                'iva': iva,
                'total': total,
                'es_nc': es_nc
            }
            reporte.append(row)
            
            totales['neto'] += neto
            totales['iva'] += iva
            totales['total'] += total

    # Nombres de meses para el selector
    meses_nombre = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
        7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }

    return render_template('contabilidad/libro_iva_ventas.html', 
                           anio=anio, mes=mes, 
                           reporte=reporte, totales=totales,
                           meses_nombre=meses_nombre)

@contabilidad_bp.route('/contabilidad/libro-iva-compras', methods=['GET'])
@login_required
def libro_iva_compras():
    today = datetime.date.today()
    anio = request.args.get('anio', today.year, type=int)
    mes = request.args.get('mes', today.month, type=int)
    
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT 
                erp_comprobantes.id, erp_comprobantes.fecha_emision, erp_comprobantes.tipo_comprobante, erp_comprobantes.punto_venta, erp_comprobantes.numero,
                erp_comprobantes.importe_neto, erp_comprobantes.importe_iva, erp_comprobantes.importe_total,
                erp_terceros.nombre as proveedor_nombre, erp_terceros.cuit as proveedor_cuit, erp_terceros.tipo_responsable,
                sys_tipos_comprobante.descripcion as tipo_desc, sys_tipos_comprobante.letra
            FROM erp_comprobantes
            JOIN erp_terceros ON erp_comprobantes.tercero_id = erp_terceros.id
            JOIN sys_tipos_comprobante ON erp_comprobantes.tipo_comprobante = sys_tipos_comprobante.codigo
            WHERE erp_comprobantes.enterprise_id = %s 
              AND erp_comprobantes.tipo_operacion = 'COMPRA'
              AND YEAR(erp_comprobantes.fecha_emision) = %s 
              AND MONTH(erp_comprobantes.fecha_emision) = %s
            ORDER BY erp_comprobantes.fecha_emision ASC, erp_comprobantes.numero ASC
        """, (g.user['enterprise_id'], anio, mes))
        
        comprobantes_db = cursor.fetchall()
        
        reporte = []
        totales = {'neto': 0.0, 'iva': 0.0, 'total': 0.0}
        
        for comp in comprobantes_db:
            es_nc = comp['tipo_comprobante'] in ['003', '008', '013']
            signo = -1 if es_nc else 1
            
            neto = float(comp['importe_neto'] or 0) * signo
            iva = float(comp['importe_iva'] or 0) * signo
            total = float(comp['importe_total'] or 0) * signo
            
            row = {
                'fecha': comp['fecha_emision'].strftime('%d/%m/%Y') if hasattr(comp['fecha_emision'], 'strftime') else comp['fecha_emision'],
                'tipo': f"{comp['tipo_desc']} ({comp['letra']})",
                'numero': f"{comp['punto_venta']:04d}-{comp['numero']:08d}",
                'proveedor': comp['proveedor_nombre'],
                'cuit': comp['proveedor_cuit'],
                'condicion': comp['tipo_responsable'],
                'neto': neto,
                'iva': iva,
                'total': total,
                'es_nc': es_nc
            }
            reporte.append(row)
            totales['neto'] += neto
            totales['iva'] += iva
            totales['total'] += total

    meses_nombre = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
        7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }

    return render_template('contabilidad/libro_iva_compras.html', 
                           anio=anio, mes=mes, 
                           reporte=reporte, totales=totales,
                           meses_nombre=meses_nombre)

@contabilidad_bp.route('/contabilidad/padrones-iibb')
@login_required
def padrones_iibb():
    stats = {}
    logs = []
    
    with get_db_cursor(dictionary=True) as cursor:
        # Estadísticas básicas
        cursor.execute("SELECT COUNT(*) as total FROM sys_padrones_iibb")
        total = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(DISTINCT jurisdiccion) as juris_cnt FROM sys_padrones_iibb")
        juris = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(DISTINCT cuit) as cuit_cnt FROM sys_padrones_iibb")
        cuits = cursor.fetchone()

        # Simulación de vigencia
        stats = {
            'total_count': total['total'],
            'juris_count': juris['juris_cnt'] if juris else 0,
            'cuit_count': cuits['cuit_cnt'] if cuits else 0,
            'vigente': total['total'] > 0
        }

        # Obtener Logs de Procesos
        try:
            cursor.execute("""
                SELECT * FROM sys_padrones_logs 
                ORDER BY fecha_ejecucion DESC 
                LIMIT 50
            """)
            logs = cursor.fetchall()
        except Exception as e:
            print(f"Error fetching logs: {e}")
            logs = []

    return render_template('contabilidad/padrones.html', stats=stats, logs=logs)

@contabilidad_bp.route('/contabilidad/api/consultar-padron/<string:cuit>')
@login_required
def api_consultar_padron(cuit):
    import asyncio
    from services.afip_service import AfipService
    cuit_clean = re.sub(r'\D', '', cuit)
    res = {'jurisdicciones': {}, 'afip': None}
    
    # 1. Consultar Padrones Locales (IIBB)
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM sys_padrones_iibb WHERE cuit = %s", (cuit_clean,))
        rows = cursor.fetchall()
        for r in rows:
            res['jurisdicciones'][r['jurisdiccion']] = {
                'alicuota_percepcion': float(r['alicuota_percepcion'] or 0),
                'alicuota_retencion': float(r['alicuota_retencion'] or 0),
                'grupo_riesgo': r['grupo_riesgo'],
                'desde': r['desde'].strftime('%Y-%m-%d') if r['desde'] else None,
                'hasta': r['hasta'].strftime('%Y-%m-%d') if r['hasta'] else None,
                'exencion_iibb': r.get('exencion_iibb', '')
            }
            
    # 2. Consultar AFIP (Real o Simulado)
    afip_res = asyncio.run(AfipService.consultar_padron(g.user['enterprise_id'], cuit_clean))
    if afip_res.get('success'):
        res['afip'] = afip_res['data']
    else:
        res['afip_error'] = afip_res.get('error')

    return jsonify(res)

@contabilidad_bp.route('/contabilidad/importar-padron/<string:jurisdiccion>', methods=['POST'])
@login_required
@atomic_transaction('contabilidad', severity=7, impact_category='Compliance')
def importar_padron(jurisdiccion):
    if 'archivo' not in request.files:
        flash("No se seleccionó archivó.", "danger")
        return redirect(url_for('contabilidad.padrones_iibb'))
    
    file = request.files['archivo']
    if file.filename == '':
        flash("Archivo vacío.", "danger")
        return redirect(url_for('contabilidad.padrones_iibb'))

    # Parseo simplificado del padrón
    # NOTA: En producción esto debería ser un task de fondo (Celery/RQ)
    try:
        content = file.read().decode('utf-8', errors='ignore')
        lines = content.splitlines()
        
        # Default dates (Current Month)
        today = datetime.date.today()
        default_desde = today.replace(day=1)
        next_month = today.replace(day=28) + datetime.timedelta(days=4)
        default_hasta = next_month.replace(day=1) - datetime.timedelta(days=1)
        
        # Mapeo de Jurisdicciones
        JURIS_MAP = {
            'AGIP': 901,
            'ARBA': 902,
            'CORDOBA': 904,
            'SANTA_FE': 921,
            'MISIONES': 914,
            'ENTRE_RIOS': 908
        }
        jurisdiccion_id = JURIS_MAP.get(jurisdiccion)
        
        if not jurisdiccion_id:
             flash(f"Jurisdicción desconocida: {jurisdiccion}", "danger")
             return redirect(url_for('contabilidad.padrones_iibb'))

        with get_db_cursor() as cursor:
            # Limpiar padrón anterior (ARBA/AGIP) para esta jurisdicción
            cursor.execute("DELETE FROM sys_padrones_iibb WHERE jurisdiccion_id = %s", (jurisdiccion_id,))
            
            records = []
            if jurisdiccion == 'ARBA':
                # Intentar detectar formato standard ARBA
                # Standard: Tipo(1), FecPub(8), FecDesde(8), FecHasta(8), CUIT(11), ...
                # Si linea larga > 100, asumimos standard
                for line in lines[:50000]:
                    if len(line) < 40: continue
                    
                    desde, hasta = default_desde, default_hasta
                    exencion = '' # Default
                    
                    # Heurística básica de formato
                    if len(line) > 100 and line[1:9].isdigit(): 
                        # Posible formato Standard ARBA
                        try:
                            d_str = line[9:17]
                            h_str = line[17:25]
                            desde = datetime.datetime.strptime(d_str, '%d%m%Y').date()
                            hasta = datetime.datetime.strptime(h_str, '%d%m%Y').date()
                            cuit = line[25:36]
                            
                            alic_perc_str = line[39:43]
                            alic_ret_str = line[43:47] 
                            
                            alic_perc = float(alic_perc_str.replace(',', '.')) / 100
                            alic_ret = float(alic_ret_str.replace(',', '.')) / 100
                            
                        except:
                            # Fallback al formato previo/custom
                            cuit = line[4:15]
                            alic_perc = float(line[34:38].replace(',', '.')) / 100 if line[34:38].strip() else 0
                            alic_ret = float(line[38:42].replace(',', '.')) / 100 if line[38:42].strip() else 0
                    else:
                        # Formato Legacy/Recortado user
                        cuit = line[4:15]
                        alic_perc = float(line[34:38].replace(',', '.')) / 100 if line[34:38].strip() else 0
                        alic_ret = float(line[38:42].replace(',', '.')) / 100 if line[38:42].strip() else 0

                    records.append((jurisdiccion, jurisdiccion_id, cuit, alic_perc, alic_ret, desde, hasta, exencion))
                    
            elif jurisdiccion == 'AGIP':
                # Formato AGIP (CSV): CUIT;...;FecDesde;FecHasta...
                # Asumimos CSV separado por ;
                for line in lines[:50000]:
                    parts = line.split(';')
                    if len(parts) < 8: continue
                    cuit = parts[2].replace('-', '').strip()
                    alic_perc = float(parts[6].replace(',', '.')) if parts[6] else 0
                    alic_ret = float(parts[5].replace(',', '.')) if parts[5] else 0
                    
                    # Intentar parsear fechas standard AGIP (Vigencia Desde/Hasta en col 3 y 4 o 0 y 1)
                    desde, hasta = default_desde, default_hasta
                    exencion = ''
                    
                    # Probamos cols 3 y 4 (DD/MM/YYYY)
                    try:
                        if len(parts) > 4:
                            d_str = parts[3].strip()
                            h_str = parts[4].strip()
                            desde = datetime.datetime.strptime(d_str, '%d/%m/%Y').date()
                            hasta = datetime.datetime.strptime(h_str, '%d/%m/%Y').date()
                    except:
                        pass # Keep defaults
                    
                    records.append((jurisdiccion, jurisdiccion_id, cuit, alic_perc, alic_ret, desde, hasta, exencion))

            elif jurisdiccion == 'SANTA_FE':
                # Parser Heurístico para Santa Fe
                # Formato Generalmente: CUIT; Tipo; Alicuota; ...
                for line in lines[:50000]:
                    # Limpieza básica
                    l = line.replace('"', '').replace("'", "")
                    parts = l.split(';')
                    if len(parts) < 3: 
                        parts = l.split(',') # Try comma
                    
                    if len(parts) < 3: continue

                    cuit = ''
                    alic_perc = 0.0
                    alic_ret = 0.0
                    
                    # Buscar CUIT
                    for p in parts:
                        clean_p = re.sub(r'\D', '', p)
                        if len(clean_p) == 11 and clean_p.startswith(('20','23','24','27','30','33')):
                            cuit = clean_p
                            break
                    
                    if not cuit: continue

                    # Buscar valores numéricos que parezcan alícuotas
                    # Asumimos que si hay dos, uno es ret y otro perc, o vienen en líneas separadas por tipo
                    # En muchos padrones provinciales viene una línea por regimen
                    
                    # Simplificación: Buscar el valor decimal más probable
                    vals = []
                    for p in parts:
                        try:
                            v = float(p.replace(',', '.'))
                            if 0 <= v <= 10: # Alícuota razonable
                                vals.append(v)
                        except: pass
                    
                    if vals:
                        # Si encontramos valores, asumimos el mayor como percepción (suelen ser mas altas) o heurística
                        # Lo ideal es detectar el código de régimen, pero sin doc específica es difícil.
                        # Ante la duda, cargamos el valor encontrado en ambas o en percepción por defecto.
                        val = max(vals)
                        alic_perc = val / 100 # Guardamos como coef (3% -> 0.03) si viene en %
                        if alic_perc > 0.15: alic_perc = val # Si era 0.03, queda 0.03. Si era 3.0, queda 3.0 (error). Asumimos viene en % (ej 3.5) -> 0.035
                        if val > 1: alic_perc = val / 100 
                        else: alic_perc = val # Ya venia en decimal 0.03
                        
                        alic_ret = alic_perc # Por defecto igual si no distinguimos

                    records.append((jurisdiccion, jurisdiccion_id, cuit, alic_perc, alic_ret, default_desde, default_hasta, ''))

            elif jurisdiccion == 'CORDOBA':
                # Parser Heurístico para Córdoba
                for line in lines[:50000]:
                    l = line.replace('"', '')
                    parts = l.split(',')
                    if len(parts) < 3: parts = l.split(';')

                    if len(parts) < 2: continue

                    cuit = ''
                    for p in parts:
                        clean_p = re.sub(r'\D', '', p)
                        if len(clean_p) == 11 and clean_p.startswith(('20','23','24','27','30','33')):
                            cuit = clean_p
                            break
                    
                    if not cuit: continue

                    alic_perc = 0.0
                    alic_ret = 0.0
                    
                    # Búsqueda de alícuota
                    try:
                        # Córdoba suele tener CSV: CUIT, Alicuota, ...
                        for p in parts:
                            try:
                                v = float(p.replace(',', '.'))
                                if 0 < v < 15: # Rango alicuota %
                                    alic_perc = v / 100
                                    alic_ret = v / 100
                                    break
                            except: continue
                    except: pass
                    
                    records.append((jurisdiccion, jurisdiccion_id, cuit, alic_perc, alic_ret, default_desde, default_hasta, ''))

            elif jurisdiccion == 'MISIONES':
                # Parser Heurístico para Misiones (ATM)
                for line in lines[:50000]:
                    l = line.replace('"', '').strip()
                    parts = l.split(';') if ';' in l else l.split(',')
                    if len(parts) < 2: continue
                    
                    cuit = ''
                    for p in parts:
                        clean_p = re.sub(r'\D', '', p)
                        if len(clean_p) == 11:
                            cuit = clean_p
                            break
                    if not cuit: continue

                    alic_perc, alic_ret = 0.0, 0.0
                    for p in parts:
                        try:
                            v = float(p.replace(',', '.'))
                            if 0 < v < 10:
                                alic_perc = v / 100 if v > 0.1 else v
                                alic_ret = alic_perc
                                break
                        except: continue
                    records.append((jurisdiccion, jurisdiccion_id, cuit, alic_perc, alic_ret, default_desde, default_hasta, ''))

            elif jurisdiccion == 'ENTRE_RIOS':
                # Parser Heurístico para Entre Ríos (ATER)
                for line in lines[:50000]:
                    l = line.replace('"', '').strip()
                    parts = l.split(';') if ';' in l else l.split(',')
                    if len(parts) < 2: continue

                    cuit = ''
                    for p in parts:
                        clean_p = re.sub(r'\D', '', p)
                        if len(clean_p) == 11:
                            cuit = clean_p
                            break
                    if not cuit: continue

                    alic_perc, alic_ret = 0.0, 0.0
                    for p in parts:
                        try:
                            v = float(p.replace(',', '.'))
                            if 0 < v < 10:
                                alic_perc = v / 100 if v > 0.1 else v
                                alic_ret = alic_perc
                                break
                        except: continue
                    records.append((jurisdiccion, jurisdiccion_id, cuit, alic_perc, alic_ret, default_desde, default_hasta, ''))

            
            if records:
                cursor.executemany("""
                    INSERT INTO sys_padrones_iibb (jurisdiccion, jurisdiccion_id, cuit, alicuota_percepcion, alicuota_retencion, desde, hasta, exencion_iibb)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, records)
            
            # Log de Proceso
            try:
                cursor.execute("""
                    INSERT INTO sys_padrones_logs 
                    (jurisdiccion, tipo_proceso, archivo_origen, registros_procesados, registros_altas, status, mensaje)
                    VALUES (%s, %s, %s, %s, %s, 'SUCCESS', 'Proceso finalizado correctamente')
                """, (jurisdiccion, 'IMPORTACION', file.filename, len(records), len(records)))
            except Exception as e:
                print(f"Error logging padron process: {e}")
                
        flash(f"Padron {jurisdiccion} procesado con éxito. ({len(records)} registros)", "success")
    except Exception as e:
        flash(f"Error procesando padrón: {str(e)}", "danger")

@contabilidad_bp.route('/contabilidad/exportar-afip', methods=['GET'])
@login_required
def exportar_afip():
    from flask import Response
    import io

    periodo = request.args.get('periodo') # YYYYMM
    if not periodo:
        return "Falta periodo", 400
    
    anio = int(periodo[:4])
    mes = int(periodo[4:])
    modulo = request.args.get('modulo', 'VENTAS')
    tipo_archivo = request.args.get('tipo', 'CBTE') # CBTE o ALICUOTAS
    
    filename = f"LIBRO_IVA_{modulo}_{tipo_archivo}_{periodo}.txt"
    output = io.StringIO()
    
    with get_db_cursor(dictionary=True) as cursor:
        if tipo_archivo == 'CBTE':
            cursor.execute("""
                SELECT erp_comprobantes.*, erp_terceros.cuit, erp_terceros.nombre, sys_tipos_comprobante.codigo as afip_tipo
                FROM erp_comprobantes
                JOIN erp_terceros ON erp_comprobantes.tercero_id = erp_terceros.id
                JOIN sys_tipos_comprobante ON erp_comprobantes.tipo_comprobante = sys_tipos_comprobante.codigo
                WHERE erp_comprobantes.enterprise_id = %s AND erp_comprobantes.modulo = %s
                  AND YEAR(erp_comprobantes.fecha_emision) = %s AND MONTH(erp_comprobantes.fecha_emision) = %s
            """, (g.user['enterprise_id'], modulo, anio, mes))
            
            for r in cursor.fetchall():
                fecha = r['fecha_emision'].strftime('%Y%m%d')
                tipo = f"{int(r['afip_tipo']):03}"
                pv = f"{int(r['punto_venta']):05}"
                nro = f"{int(r['numero']):020}"
                cuit = re.sub(r'\D', '', r['cuit']).rjust(20, '0')
                nombre = (r['nombre'][:30]).ljust(30)
                
                total = f"{int(round(float(r['importe_total'] or 0) * 100)):015}"
                neto = f"{int(round(float(r['importe_neto'] or 0) * 100)):015}"
                iva = f"{int(round(float(r['importe_iva'] or 0) * 100)):015}"
                
                # Formato simplificado compatible con AFIP Libro IVA Digital
                line = f"{fecha}{tipo}{pv}{nro}{nro}80{cuit}{nombre}{total}{'0'*15}{'0'*15}{'0'*15}{'0'*15}{'0'*15}{iva}{'0'*15}"
                output.write(line + "\r\n")
        
        else: # ALICUOTAS
            cursor.execute("""
                SELECT d.alicuota_iva, d.subtotal_neto, d.importe_iva, 
                       c.tipo_comprobante as afip_tipo, c.punto_venta, c.numero
                FROM erp_comprobantes_detalle d
                JOIN erp_comprobantes c ON d.comprobante_id = c.id
                WHERE c.enterprise_id = %s AND c.modulo = %s
                  AND YEAR(c.fecha_emision) = %s AND MONTH(c.fecha_emision) = %s
            """, (g.user['enterprise_id'], modulo, anio, mes))
            
            # Mapeo Alicuotas AFIP
            # 5: 21%, 4: 10.5%, 6: 27%, 8: 5%, 9: 2.5%, 3: 0%
            map_ali = {21.0: '0005', 10.5: '0004', 27.0: '0006', 5.0: '0008', 2.5: '0009', 0.0: '0003'}
            
            for r in cursor.fetchall():
                tipo = f"{int(r['afip_tipo']):03}"
                pv = f"{int(r['punto_venta']):05}"
                nro = f"{int(r['numero']):020}"
                neto = f"{int(round(float(r['subtotal_neto'] or 0) * 100)):015}"
                ali_code = map_ali.get(float(r['alicuota_iva']), '0005')
                iva = f"{int(round(float(r['importe_iva'] or 0) * 100)):015}"
                
                line = f"{tipo}{pv}{nro}{neto}{ali_code}{iva}"
                output.write(line + "\r\n")

    return Response(
        output.getvalue(),
        mimetype="text/plain",
        headers={"Content-disposition": f"attachment; filename={filename}"}
    )

@contabilidad_bp.route('/contabilidad/reporte-iibb')
@login_required
def reporte_iibb():
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Percepciones sufridas (en Compras)
        # 2. Retenciones sufridas (en Cobros - No implementado todavía)
        # 3. Retenciones practicadas (en Pagos - Implementado en fin_retenciones_emitidas)
        
        cursor.execute("""
            SELECT sys_jurisdicciones.codigo, sys_jurisdicciones.nombre, 
                   COALESCE(SUM(erp_comprobantes.importe_neto), 0) as base,
                   COALESCE(SUM(erp_comprobantes.importe_percepcion_iibb_arba + erp_comprobantes.importe_percepcion_iibb_agip), 0) as percepciones,
                   COALESCE((SELECT SUM(importe_retencion) FROM fin_retenciones_emitidas WHERE enterprise_id = %s AND (jurisdiccion_id = sys_jurisdicciones.codigo OR (tipo_retencion='IIBB' AND sys_jurisdicciones.codigo=902))), 0) as retenciones
            FROM sys_jurisdicciones
            LEFT JOIN erp_comprobantes ON erp_comprobantes.jurisdiccion_id = sys_jurisdicciones.codigo AND erp_comprobantes.enterprise_id = %s
            GROUP BY sys_jurisdicciones.codigo, sys_jurisdicciones.nombre
            HAVING COALESCE(SUM(erp_comprobantes.importe_neto), 0) > 0 
                OR COALESCE(SUM(erp_comprobantes.importe_percepcion_iibb_arba + erp_comprobantes.importe_percepcion_iibb_agip), 0) > 0 
                OR COALESCE((SELECT SUM(importe_retencion) FROM fin_retenciones_emitidas WHERE enterprise_id = %s AND (jurisdiccion_id = sys_jurisdicciones.codigo OR (tipo_retencion='IIBB' AND sys_jurisdicciones.codigo=902))), 0) > 0
            ORDER BY sys_jurisdicciones.codigo
        """, (g.user['enterprise_id'], g.user['enterprise_id'], g.user['enterprise_id']))
        
        reporte = cursor.fetchall()
        
        totales = {
            'base': sum(float(r['base']) for r in reporte),
            'percepciones': sum(float(r['percepciones']) for r in reporte),
            'retenciones': sum(float(r['retenciones']) for r in reporte)
        }
        
    import datetime
    return render_template('contabilidad/reporte_iibb.html', 
                            reporte=reporte, 
                            totales=totales,
                            current_month=datetime.date.today().strftime('%Y-%m'))

@contabilidad_bp.route('/contabilidad/exportar-sicore', methods=['GET'])
@login_required
def exportar_sicore():
    from flask import Response
    import io
    
    periodo = request.args.get('periodo', datetime.date.today().strftime('%Y%m')) # YYYYMM
    tipo = request.args.get('tipo', 'SICORE') # SICORE o SIFERE
    
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT cuit FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        empresa = cursor.fetchone()
        
        # Naming convention: CUIT_PERIODO_MM_YYYY_FEC_EMIS_DDMMYYYYHHMMSS.txt
        cuit_clean = re.sub(r'\D', '', empresa['cuit'])
        ahora = datetime.datetime.now()
        filename = f"{cuit_clean}_PERIODO_{periodo[4:6]}_{periodo[:4]}_FEC_EMIS_{ahora.strftime('%d%m%Y%H%M%S')}.txt"
        
        output = io.StringIO()
        
        # Obtener retenciones del periodo
        cursor.execute("""
            SELECT fin_retenciones_emitidas.*, erp_terceros.cuit as sujeto_cuit, erp_terceros.nombre as sujeto_nombre
            FROM fin_retenciones_emitidas
            JOIN erp_terceros ON fin_retenciones_emitidas.tercero_id = erp_terceros.id
            WHERE fin_retenciones_emitidas.enterprise_id = %s AND DATE_FORMAT(fin_retenciones_emitidas.fecha, '%%Y%%m') = %s
        """, (g.user['enterprise_id'], periodo))
        
        retenciones = cursor.fetchall()
        
        for r in retenciones:
            # Formatos específicos (Ejemplo SICORE simplificado)
            # Código Impuesto (ej 030), Código Régimen (ej 775), Fecha, CUIT Sujeto, Monto
            fecha_str = r['fecha'].strftime('%d/%m/%Y')
            cuit_sujeto = re.sub(r'\D', '', r['sujeto_cuit']).rjust(11, '0')
            monto = f"{float(r['importe_retencion']):15.2f}".replace('.', ',').strip().rjust(15, '0')
            
            if tipo == 'SICORE':
                # Ejemplo de línea SICORE (Estructura fija según AFIP)
                line = f"030775{fecha_str}{cuit_sujeto}{monto}"
            else:
                # Ejemplo SIFERE (Provincial)
                line = f"902{fecha_str}{cuit_sujeto}{monto}"
            
            output.write(line + "\r\n")
            
    return Response(
        output.getvalue(),
        mimetype="text/plain",
        headers={"Content-disposition": f"attachment; filename={filename}"}
    )

@contabilidad_bp.route('/contabilidad/plan-cuentas')
@login_required
def plan_cuentas():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, codigo, nombre, tipo, nivel, es_analitica 
            FROM cont_plan_cuentas 
            WHERE enterprise_id = %s 
            ORDER BY codigo
        """, (g.user['enterprise_id'],))
        cuentas = cursor.fetchall()
    return render_template('contabilidad/plan_cuentas.html', cuentas=cuentas)

@contabilidad_bp.route('/contabilidad/libro-diario')
@login_required
def libro_diario():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT cont_asientos.*, 
                   (SELECT SUM(debe) FROM cont_asientos_detalle WHERE asiento_id = cont_asientos.id) as total_debe,
                   (SELECT SUM(haber) FROM cont_asientos_detalle WHERE asiento_id = cont_asientos.id) as total_haber
            FROM cont_asientos 
            WHERE cont_asientos.enterprise_id = %s 
            ORDER BY cont_asientos.fecha DESC, cont_asientos.id DESC
        """, (g.user['enterprise_id'],))
        asientos = cursor.fetchall()
    return render_template('contabilidad/libro_diario.html', asientos=asientos)

@contabilidad_bp.route('/contabilidad/asiento/<int:id>')
@login_required
def ver_asiento(id):
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM cont_asientos WHERE id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        asiento = cursor.fetchone()
        if not asiento:
            flash("Asiento no encontrado", "danger")
            return redirect(url_for('contabilidad.libro_diario'))
        
        cursor.execute("""
            SELECT cont_asientos_detalle.*, cont_plan_cuentas.codigo as cuenta_codigo, cont_plan_cuentas.nombre as cuenta_nombre
            FROM cont_asientos_detalle
            JOIN cont_plan_cuentas ON cont_asientos_detalle.cuenta_id = cont_plan_cuentas.id
            WHERE cont_asientos_detalle.asiento_id = %s
        """, (id,))
        detalles = cursor.fetchall()
    return render_template('contabilidad/ver_asiento.html', asiento=asiento, detalles=detalles)

@contabilidad_bp.route('/contabilidad/centralizacion', methods=['GET', 'POST'])
@login_required
@atomic_transaction('contabilidad', severity=8, impact_category='Financial')
def centralizacion():
    today = datetime.date.today()
    meses_nombre = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
        7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }

    if request.method == 'POST':
        modulo = request.form.get('modulo')
        mes = int(request.form.get('mes'))
        anio = int(request.form.get('anio'))
        
        try:
            res = _generar_asiento_resumen(modulo, mes, anio)
            if res:
                flash(f"Centralización de {modulo} realizada con éxito. Asiento #{res}", "success")
            else:
                flash(f"No hay comprobantes pendientes de centralizar para {modulo} en {mes}/{anio}", "warning")
        except Exception as e:
            flash(f"Error en centralización: {str(e)}", "danger")
        
        return redirect(url_for('contabilidad.centralizacion'))

    # GET: Mostrar resumen de pendientes
    pendientes = []
    with get_db_cursor(dictionary=True) as cursor:
        # Pendientes Ventas
        cursor.execute("""
            SELECT 'VENTAS' as modulo, COUNT(*) as cantidad, SUM(importe_total) as total 
            FROM erp_comprobantes 
            WHERE enterprise_id = %s AND modulo = 'VENTAS' AND asiento_id IS NULL
        """, (g.user['enterprise_id'],))
        pendientes.append(cursor.fetchone())
        
        # Pendientes Compras
        cursor.execute("""
            SELECT 'COMPRAS' as modulo, COUNT(*) as cantidad, SUM(importe_total) as total 
            FROM erp_comprobantes 
            WHERE enterprise_id = %s AND modulo = 'COMPRAS' AND asiento_id IS NULL
        """, (g.user['enterprise_id'],))
        pendientes.append(cursor.fetchone())

        # Pendientes Fondos
        cursor.execute("""
            SELECT 'FONDOS' as modulo, COUNT(*) as cantidad, SUM(importe) as total 
            FROM erp_movimientos_fondos 
            WHERE enterprise_id = %s AND asiento_id IS NULL
        """, (g.user['enterprise_id'],))
        pendientes.append(cursor.fetchone())
        
        # Pendientes Sueldos
        cursor.execute("""
            SELECT 'SUELDOS' as modulo, COUNT(*) as cantidad, SUM(total_neto) as total 
            FROM fin_nominas 
            WHERE enterprise_id = %s AND asiento_id IS NULL
        """, (g.user['enterprise_id'],))
        pendientes.append(cursor.fetchone())

    return render_template('contabilidad/centralizacion.html', 
                         meses_nombre=meses_nombre, 
                         today_mes=today.month, 
                         today_anio=today.year,
                         pendientes=pendientes)

def _get_cuenta_id(cursor, enterprise_id, codigo):
    cursor.execute("SELECT id FROM cont_plan_cuentas WHERE enterprise_id = %s AND codigo = %s", (enterprise_id, codigo))
    res = cursor.fetchone()
    if not res:
        # Fallback to global
        cursor.execute("SELECT id FROM cont_plan_cuentas WHERE (enterprise_id = 0 OR enterprise_id = 1) AND codigo = %s LIMIT 1", (codigo,))
        res = cursor.fetchone()
    return res['id'] if res else None

def _generar_asiento_resumen(modulo, mes, anio):
    enterprise_id = g.user['enterprise_id']
    
    with get_db_cursor(dictionary=True) as cursor:
        if modulo == 'VENTAS':
            cursor.execute("""
                SELECT id, importe_neto, importe_iva, importe_total, tipo_comprobante
                FROM erp_comprobantes 
                WHERE enterprise_id = %s AND modulo = 'VENTAS' AND asiento_id IS NULL
                AND MONTH(fecha_emision) = %s AND YEAR(fecha_emision) = %s
            """, (enterprise_id, mes, anio))
            comprobantes = cursor.fetchall()
            
            if not comprobantes: return None
            
            total_neto = 0
            total_iva = 0
            total_total = 0
            ids = []
            
            for c in comprobantes:
                es_nc = c['tipo_comprobante'] in ['003', '008', '013']
                signo = -1 if es_nc else 1
                total_neto += float(c['importe_neto'] or 0) * signo
                total_iva += float(c['importe_iva'] or 0) * signo
                total_total += float(c['importe_total'] or 0) * signo
                ids.append(c['id'])
            
            # Crear Asiento
            fecha_asiento = datetime.date(anio, mes, 1) # Simplificado al dia 1 o ultimo del mes
            # Ultimo dia del mes
            if mes == 12:
                fecha_asiento = datetime.date(anio, 12, 31)
            else:
                fecha_asiento = datetime.date(anio, mes + 1, 1) - datetime.timedelta(days=1)

            cursor.execute("""
                INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (enterprise_id, fecha_asiento, f"Centralización Ventas {mes}/{anio}", 'VENTAS', g.user['id']))
            asiento_id = cursor.lastrowid
            
            # Detalle
            cta_deudores = _get_cuenta_id(cursor, enterprise_id, '1.3.01')
            cta_ventas = _get_cuenta_id(cursor, enterprise_id, '4.1')
            cta_iva_db = _get_cuenta_id(cursor, enterprise_id, '2.2.01')
            
            # Partida Doble: Ventas es HABER, Deudores es DEBE
            # Si total_total > 0: DEBE Deudores, HABER Ventas, HABER IVA
            # DEBE
            # DEBE
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (asiento_id, cta_deudores, total_total if total_total > 0 else 0, abs(total_total) if total_total < 0 else 0, "Deudores por Ventas", enterprise_id, g.user['id']))
            # HABER Ventas
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (asiento_id, cta_ventas, abs(total_neto) if total_neto < 0 else 0, total_neto if total_neto > 0 else 0, "Ventas del periodo", enterprise_id, g.user['id']))
            # HABER IVA
            if total_iva != 0:
                cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                               (asiento_id, cta_iva_db, abs(total_iva) if total_iva < 0 else 0, total_iva if total_iva > 0 else 0, "IVA Débito Fiscal", enterprise_id, g.user['id']))
            
            # Actualizar links
            format_strings = ','.join(['%s'] * len(ids))
            cursor.execute(f"UPDATE erp_comprobantes SET asiento_id = %s WHERE id IN ({format_strings})", [asiento_id] + ids)
            
            return asiento_id

        elif modulo == 'COMPRAS':
            cursor.execute("""
                SELECT id, importe_neto, importe_iva, importe_total, tipo_comprobante
                FROM erp_comprobantes 
                WHERE enterprise_id = %s AND modulo = 'COMPRAS' AND asiento_id IS NULL
                AND MONTH(fecha_emision) = %s AND YEAR(fecha_emision) = %s
            """, (enterprise_id, mes, anio))
            comprobantes = cursor.fetchall()
            
            if not comprobantes: return None
            
            total_neto = 0
            total_iva = 0
            total_total = 0
            ids = []
            
            for c in comprobantes:
                es_nc = c['tipo_comprobante'] in ['003', '008', '013']
                signo = -1 if es_nc else 1
                total_neto += float(c['importe_neto'] or 0) * signo
                total_iva += float(c['importe_iva'] or 0) * signo
                total_total += float(c['importe_total'] or 0) * signo
                ids.append(c['id'])

            fecha_asiento = datetime.date(anio, mes + 1, 1) - datetime.timedelta(days=1) if mes < 12 else datetime.date(anio, 12, 31)

            cursor.execute("""
                INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (enterprise_id, fecha_asiento, f"Centralización Compras {mes}/{anio}", 'COMPRAS', g.user['id']))
            asiento_id = cursor.lastrowid
            
            # Detalle Compras
            # IVA Credito (Si no existe 1.5.01, usamos 2.2.01 negativo o creamos uno)
            # Para este MVP usaremos 2.2.02 (IVA a Pagar) o buscaremos uno de Activo.
            cta_gastos = _get_cuenta_id(cursor, enterprise_id, '5.2') # Gastos Admin genérico
            cta_iva_cr = _get_cuenta_id(cursor, enterprise_id, '1.5.01') # Suponiendo que existe
            if not cta_iva_cr: cta_iva_cr = _get_cuenta_id(cursor, enterprise_id, '2.2.01') # Fallback
            
            cta_proveedores = _get_cuenta_id(cursor, enterprise_id, '2.1.01')
            
            # HABER Proveedores
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (asiento_id, cta_proveedores, abs(total_total) if total_total < 0 else 0, total_total if total_total > 0 else 0, "Proveedores (Acreedores)", enterprise_id, g.user['id']))
            # DEBE Gastos/Mercaderia
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (asiento_id, cta_gastos, total_neto if total_neto > 0 else 0, abs(total_neto) if total_neto < 0 else 0, "Compras de Bienes/Servicios", enterprise_id, g.user['id']))
            # DEBE IVA
            if total_iva != 0:
                cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                               (asiento_id, cta_iva_cr, total_iva if total_iva > 0 else 0, abs(total_iva) if total_iva < 0 else 0, "IVA Crédito Fiscal", enterprise_id, g.user['id']))
            
            format_strings = ','.join(['%s'] * len(ids))
            cursor.execute(f"UPDATE erp_comprobantes SET asiento_id = %s WHERE id IN ({format_strings})", [asiento_id] + ids)
            return asiento_id

        elif modulo == 'FONDOS':
            # Centralizar Ingresos y Egresos de Tesorería
            cursor.execute("""
                SELECT m.*, cf.cuenta_contable_id
                FROM erp_movimientos_fondos m
                JOIN erp_cuentas_fondos cf ON m.cuenta_fondo_id = cf.id
                WHERE m.enterprise_id = %s AND m.asiento_id IS NULL
                AND MONTH(m.fecha) = %s AND YEAR(m.fecha) = %s
            """, (enterprise_id, mes, anio))
            movimientos = cursor.fetchall()
            
            if not movimientos: return None
            
            # Agrupar por cuenta contable
            saldos = {} # {cuenta_id: {'debe': X, 'haber': Y}}
            ids = []
            
            # Cuentas default para contrapartida
            cta_deudores = _get_cuenta_id(cursor, enterprise_id, '1.3.01')
            cta_proveedores = _get_cuenta_id(cursor, enterprise_id, '2.1.01')
            
            for m in movimientos:
                importe = float(m['importe'])
                ctid = m['cuenta_contable_id']
                ids.append(m['id'])
                
                if ctid not in saldos: saldos[ctid] = {'debe': 0, 'haber': 0}
                
                if m['tipo'] == 'INGRESO':
                    # Entra plata a la caja (DEBE)
                    saldos[ctid]['debe'] += importe
                    # Sale de deuda de cliente (HABER)
                    if cta_deudores not in saldos: saldos[cta_deudores] = {'debe': 0, 'haber': 0}
                    saldos[cta_deudores]['haber'] += importe
                else:
                    # Sale plata de la caja (HABER)
                    saldos[ctid]['haber'] += importe
                    # Se cancela deuda con proveedor (DEBE)
                    if cta_proveedores not in saldos: saldos[cta_proveedores] = {'debe': 0, 'haber': 0}
                    saldos[cta_proveedores]['debe'] += importe

            fecha_asiento = datetime.date(anio, mes + 1, 1) - datetime.timedelta(days=1) if mes < 12 else datetime.date(anio, 12, 31)
            cursor.execute("""
                INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (enterprise_id, fecha_asiento, f"Centralización Tesorería {mes}/{anio}", 'FONDOS', g.user['id']))
            asiento_id = cursor.lastrowid
            
            for ctid, values in saldos.items():
                if values['debe'] != 0 or values['haber'] != 0:
                    cursor.execute("""
                        INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (asiento_id, ctid, values['debe'], values['haber'], "Movimientos de Tesorería", enterprise_id, g.user['id']))
            
            format_strings = ','.join(['%s'] * len(ids))
            cursor.execute(f"UPDATE erp_movimientos_fondos SET asiento_id = %s WHERE id IN ({format_strings})", [asiento_id] + ids)
            return asiento_id

    return None

# --- MODULO DE SUELDOS (NOMINAS) ---

@contabilidad_bp.route('/contabilidad/sueldos')
@login_required
def sueldos_dashboard():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM fin_nominas WHERE enterprise_id = %s ORDER BY periodo DESC", (g.user['enterprise_id'],))
        nominas = cursor.fetchall()
    return render_template('contabilidad/sueldos.html', nominas=nominas)

@contabilidad_bp.route('/contabilidad/liquidar-sueldos', methods=['POST'])
@login_required
@atomic_transaction('contabilidad', severity=9, impact_category='Financial')
def liquidar_sueldos():
    periodo = request.form.get('periodo') # YYYY-MM
    descripcion = request.form.get('descripcion', f"Liquidación {periodo}")
    
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Crear cabecera de nómina
        cursor.execute("""
            INSERT INTO fin_nominas (enterprise_id, periodo, descripcion, estado)
            VALUES (%s, %s, %s, 'LIQUIDADO')
        """, (g.user['enterprise_id'], periodo, descripcion))
        nomina_id = cursor.lastrowid
        
        # 2. Obtener empleados (usuarios con rol de empleado o similar)
        # Por simplificacion, liquidamos a TODOS los usuarios activos de la empresa
        cursor.execute("SELECT id FROM sys_users WHERE enterprise_id = %s", (g.user['enterprise_id'],))
        empleados = cursor.fetchall()
        
        total_bruto = 0
        total_neto = 0
        
        # 3. Generar liquidaciones ficticias (MVP)
        for emp in empleados:
            bruto = 100000.0 # Valor base de ejemplo
            retenciones = bruto * 0.17 # 17% aportes
            neto = bruto - retenciones
            
            cursor.execute("""
                INSERT INTO fin_liquidaciones (enterprise_id, nomina_id, usuario_id, sueldo_bruto, retenciones, neto_a_cobrar)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], nomina_id, emp['id'], bruto, retenciones, neto))
            
            total_bruto += bruto
            total_neto += neto
            
        # Actualizar totales en cabecera
        cursor.execute("UPDATE fin_nominas SET total_bruto = %s, total_neto = %s WHERE id = %s", (total_bruto, total_neto, nomina_id))
        
        flash(f"Nómina de {periodo} generada correctamente para {len(empleados)} empleados.", "success")
        
    return redirect(url_for('contabilidad.sueldos_dashboard'))

@contabilidad_bp.route('/contabilidad/centralizar-sueldos/<int:id>', methods=['POST'])
@login_required
@atomic_transaction('contabilidad', severity=9, impact_category='Financial')
def centralizar_sueldos(id):
    enterprise_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM fin_nominas WHERE id = %s AND enterprise_id = %s AND asiento_id IS NULL", (id, enterprise_id))
        nomina = cursor.fetchone()
        
        if not nomina:
            flash("Nómina no encontrada o ya centralizada.", "warning")
            return redirect(url_for('contabilidad.sueldos_dashboard'))
        
        # Crear Asiento
        cta_sueldos_gasto = _get_cuenta_id(cursor, enterprise_id, '5.2.01') # Sueldos y Jornales
        cta_sueldos_pagar = _get_cuenta_id(cursor, enterprise_id, '2.1.01') # Proveedores/Acreedores genérico o crear cta especifica
        # Si no existe cta especifica de Sueldos a Pagar, usamos una transitoria
        
        cursor.execute("""
            INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, user_id)
            VALUES (%s, NOW(), %s, 'SUELDOS', %s)
        """, (enterprise_id, f"Asiento Sueldos {nomina['periodo']}", g.user['id']))
        asiento_id = cursor.lastrowid
        
        # DEBE: Gasto
        cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (asiento_id, cta_sueldos_gasto, nomina['total_bruto'], 0, "Sueldos Brutos Periodo", enterprise_id, g.user['id']))
        
        # HABER: Sueldos a Pagar (Neto)
        cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (asiento_id, cta_sueldos_pagar, 0, nomina['total_neto'], "Sueldos a Pagar (Neto)", enterprise_id, g.user['id']))
        
        # HABER: Retenciones a Pagar (Diferencia)
        ret_val = float(nomina['total_bruto']) - float(nomina['total_neto'])
        if ret_val > 0:
            cta_retenciones = _get_cuenta_id(cursor, enterprise_id, '2.2.02') # IVA/Fiscas a Pagar genérico
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, glosa, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (asiento_id, cta_retenciones, 0, ret_val, "Aportes y Contribuciones a Pagar", enterprise_id, g.user['id']))
        
        cursor.execute("UPDATE fin_nominas SET asiento_id = %s, estado = 'CONTABILIZADO' WHERE id = %s", (asiento_id, id))
        flash(f"Nómina centralizada con Asiento #{asiento_id}", "success")
        
    return redirect(url_for('contabilidad.sueldos_dashboard'))
