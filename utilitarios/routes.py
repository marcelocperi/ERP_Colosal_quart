from quart import Blueprint, render_template, request, jsonify, g, flash, redirect, url_for
from database import get_db_cursor
import datetime
import json

utilitarios_bp = Blueprint('utilitarios', __name__, template_folder='templates')

def calcular_proxima_ejecucion(frecuencia, planificacion_str):
    """Calcula la próxima ejecución a partir de ahora según la planificación."""
    import json
    from datetime import datetime, timedelta
    try:
        now = datetime.now()
        plan = json.loads(planificacion_str) if isinstance(planificacion_str, str) else planificacion_str
        if not plan:
            return None

        if frecuencia == 'semanal' and plan.get('days'):
            days = [int(d) for d in plan['days']]  # 1=Lun ... 7=Dom
            hour, minute = map(int, plan.get('hour', '00:00').split(':'))
            # Buscar el próximo día de semana válido (hoy inclusive si la hora no pasó)
            for offset in range(0, 8):
                candidate = now + timedelta(days=offset)
                # isoweekday(): 1=Mon ... 7=Sun
                if candidate.isoweekday() in days:
                    candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if candidate > now:
                        return candidate.strftime('%Y-%m-%d %H:%M:%S')

        elif frecuencia == 'diaria' and plan.get('hour'):
            hour, minute = map(int, plan['hour'].split(':'))
            candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            return candidate.strftime('%Y-%m-%d %H:%M:%S')

        elif frecuencia == 'minutos' and plan.get('minutes'):
            mins = int(plan['minutes'])
            candidate = now + timedelta(minutes=mins)
            return candidate.strftime('%Y-%m-%d %H:%M:%S')

    except Exception:
        pass
    return None


@utilitarios_bp.route('/utilitarios/crons')
async def gestor_crons():
    from datetime import datetime
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_crons WHERE enterprise_id = %s OR enterprise_id = 0 ORDER BY id DESC", (g.user['enterprise_id'],))
        crons = await cursor.fetchall()
    now = datetime.now()
    for cron in crons:
        proxima = cron.get('proxima_ejecucion')
        # Recalcular si es None o está en el pasado
        if proxima is None or (hasattr(proxima, 'timestamp') and proxima < now) or \
           (isinstance(proxima, str) and proxima < now.strftime('%Y-%m-%d %H:%M:%S')):
            cron['proxima_ejecucion'] = calcular_proxima_ejecucion(cron.get('frecuencia'), cron.get('planificacion'))
        # Serializar fechas restantes para tojson
        for key, val in list(cron.items()):
            if hasattr(val, 'isoformat'):
                cron[key] = val.isoformat()
    return await render_template('utilitarios/crons.html', crons=crons)

@utilitarios_bp.route('/utilitarios/crons/run/<int:cron_id>', methods=['POST'])
async def run_cron(cron_id):
    """Ejecuta forzadamente un cron por su ID y registra el resultado."""
    import subprocess, sys, os, threading
    from datetime import datetime
    from database import calcular_proxima_ejecucion

    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_crons WHERE id = %s AND (enterprise_id = %s OR enterprise_id = 0)",
                       (cron_id, g.user['enterprise_id']))
        cron = await cursor.fetchone()

    if not cron:
        return jsonify({"success": False, "error": "Cron no encontrado"}), 404

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    start_time = datetime.now()

    # Crear entrada de log previa
    async with get_db_cursor() as cursor:
        await cursor.execute("""
            INSERT INTO sys_crons_logs (cron_id, fecha_inicio, status, resultado)
            VALUES (%s, %s, %s, %s)
        """, (cron_id, start_time, 'exito', 'Iniciando ejecución forzada...'))
        log_id = cursor.lastrowid
        # El commit usualmente es automático en el context manager o se hace vía connection
        # await cursor.connection.commit() 

    async def _run():
        try:
            cmd = cron['comando'].split()
            env = os.environ.copy()
            env['CRON_UI_MODE'] = '1'
            result = subprocess.run(
                [sys.executable] + cmd[1:] if cmd[0] in ('python', 'python3') else cmd,
                cwd=project_root,
                capture_output=True, text=True, timeout=300, encoding='utf-8', env=env
            )
            end_time = datetime.now()
            status = 'exito' if result.returncode == 0 else 'error'
            output = (result.stdout or '') + (result.stderr or '')
            proxima = calcular_proxima_ejecucion(cron['frecuencia'], cron['planificacion'])
            async with get_db_cursor() as c:
                await c.execute("""
                    UPDATE sys_crons_logs SET fecha_fin=%s, status=%s, resultado=%s WHERE id=%s
                """, (end_time, status, output[:2000] or 'Sin salida', log_id))
                await c.execute("""
                    UPDATE sys_crons SET ultima_ejecucion=%s, proxima_ejecucion=%s WHERE id=%s
                """, (end_time, proxima, cron_id))
                # await c.connection.commit()
        except Exception as ex:
            async with get_db_cursor() as c:
                await c.execute("UPDATE sys_crons_logs SET fecha_fin=%s, status=%s, resultado=%s WHERE id=%s",
                          (datetime.now(), 'error', str(ex), log_id))
                # await c.connection.commit()

    import asyncio
    asyncio.create_task(_run())
    return jsonify({"success": True, "log_id": log_id, "message": "Cron iniciado en background"})

@utilitarios_bp.route('/utilitarios/crons/api/logs/<int:cron_id>')
async def get_cron_logs(cron_id):
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_crons_logs WHERE cron_id = %s ORDER BY fecha_inicio DESC LIMIT 50", (cron_id,))
        logs = await cursor.fetchall()
        
        # Format dates for JSON
        for log in logs:
            if log['fecha_inicio']: log['fecha_inicio'] = log['fecha_inicio'].strftime("%Y-%m-%d %H:%M:%S")
            if log['fecha_fin']: log['fecha_fin'] = log['fecha_fin'].strftime("%Y-%m-%d %H:%M:%S")
            
    return jsonify(logs)

@utilitarios_bp.route('/utilitarios/crons/save', methods=['POST'])
async def save_cron():
    from database import calcular_proxima_ejecucion
    data = (await request.form)
    cron_id = data.get('id')
    nombre = data.get('nombre')
    descripcion = data.get('descripcion')
    comando = data.get('comando')
    frecuencia = data.get('frecuencia') or 'diaria'

    planificacion = {
        'days': (await request.form).getlist('days[]'),
        'hour': data.get('hour'),
        'minutes': data.get('minutes')
    }
    proxima = calcular_proxima_ejecucion(frecuencia, planificacion)

    async with get_db_cursor() as cursor:
        if cron_id:
            await cursor.execute("""
                UPDATE sys_crons 
                SET nombre=%s, descripcion=%s, comando=%s, frecuencia=%s, planificacion=%s, proxima_ejecucion=%s
                WHERE id=%s AND (enterprise_id=%s OR enterprise_id=0)
            """, (nombre, descripcion, comando, frecuencia, json.dumps(planificacion), proxima, cron_id, g.user['enterprise_id']))
        else:
            await cursor.execute("""
                INSERT INTO sys_crons (nombre, descripcion, comando, frecuencia, planificacion, proxima_ejecucion, enterprise_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (nombre, descripcion, comando, frecuencia, json.dumps(planificacion), proxima, g.user['enterprise_id']))

        # await cursor.connection.commit()

    await flash("Cron guardado correctamente", "success")
    return redirect(url_for('utilitarios.gestor_crons'))

@utilitarios_bp.route('/utilitarios/crons/delete/<int:cron_id>', methods=['POST'])
async def delete_cron(cron_id):
    async with get_db_cursor() as cursor:
        await cursor.execute("DELETE FROM sys_crons WHERE id=%s AND (enterprise_id=%s OR enterprise_id=0)", (cron_id, g.user['enterprise_id']))
        # await cursor.connection.commit()
    return jsonify({"success": True})

# --- Rutas para Firma Digital de QZ Tray ---
import os
import qz_auth
from quart import send_file, request

@utilitarios_bp.route('/api/qz/cert')
def qz_cert():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cert_path = os.path.join(base_dir, 'qz_cert.pem')
    # Generar si no existe
    if not os.path.exists(cert_path):
        qz_auth.generate_qz_keys(base_dir)
    return send_file(cert_path, mimetype='text/plain')

@utilitarios_bp.route('/api/qz/sign', methods=['GET', 'POST'])
async def qz_sign():
    # QZ envía un string "request" que debemos firmar
    if request.method == 'POST':
        message = (await request.form).get('request') or (await request.json).get('request')
    else:
        message = request.args.get('request')
        
    if not message:
        return "Message to sign not found", 400
        
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
        signature = qz_auth.sign_message(base_dir, message)
        return signature
    except Exception as e:
        return str(e), 500
