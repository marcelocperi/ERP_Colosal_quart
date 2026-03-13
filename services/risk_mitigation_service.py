
import logging
import datetime
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database import get_db_cursor
try:
    from services.email_service import _enviar_email, _generar_html_template
except ImportError:
    from email_service import _enviar_email, _generar_html_template

logger = logging.getLogger(__name__)

async def process_mitigation(transaction_data):
    """
    Analiza si una transacción requiere una respuesta de mitigación activa (FMECA).
    
    Args:
        transaction_data: Dict con metadata de la transacción (ent_id, sev, mode, etc.)
    """
    ent_id = transaction_data.get('enterprise_id', 0)
    severity = transaction_data.get('severity', 0)
    failure_mode = transaction_data.get('failure_mode')
    
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # 1. Buscar reglas que apliquen a esta empresa o globales (0)
            # Filtramos por severidad y modo de falla (si la regla lo especifica)
            query = """
                SELECT * FROM sys_risk_mitigation_rules 
                WHERE (enterprise_id = %s OR enterprise_id = 0) 
                AND is_active = 1
                AND (failure_mode IS NULL OR failure_mode = %s)
                AND min_severity <= %s
            """
            await cursor.execute(query, (ent_id, failure_mode, severity))
            rules = await cursor.fetchall()
            
            for rule in rules:
                await _execute_mitigation_action(rule, transaction_data, cursor)
                
    except Exception as e:
        logger.error(f"⚠️ Error en Mitigación Activa: {e}")

async def _execute_mitigation_action(rule, data, cursor):
    """Ejecuta la acción definida en la regla."""
    action = rule['action_type']
    ent_id = data.get('enterprise_id', 0)
    user_id = data.get('user_id')
    
    # Registrar que se está tomando una acción
    await cursor.execute("""
        INSERT INTO sys_risk_active_mitigations (enterprise_id, rule_id, target_user_id, action_taken)
        VALUES (%s, %s, %s, %s)
    """, (ent_id, rule['id'], user_id, action))
    
    if action == 'ALERT_EMAIL':
        await _send_risk_alert_email(rule, data)
    elif action == 'LOG_CRITICAL':
        logger.critical(f"🔥 FMECA RISK ALERT: {data}")
    elif action == 'BLOCK_USER' and user_id:
        # Aquí se podría implementar una lógica para marcar al usuario como 'suspendido' temporalmente
        logger.warning(f"🚫 BLOCK_USER mitigación activada para usuario {user_id} en empresa {ent_id}")

async def _send_risk_alert_email(rule, data):
    """Envía alerta por correo al administrador responsable."""
    recipient = rule['recipient_email'] or "admin@biblioteca.com"
    subject = f"⚠️ ALERTA DE RIESGO: {data['module'].upper()} - Falla: {data['failure_mode'] or 'TECHNICAL'}"
    
    # Determinar color basado en severidad
    color = "#f43f5e" if data['severity'] >= 9 else "#f59e0b"
    
    detalles = {
        "Empresa ID": data.get('enterprise_id'),
        "Módulo": data.get('module'),
        "Endpoint": data.get('endpoint'),
        "Impacto": data.get('impact_category'),
        "Severidad": f"{data.get('severity')}/10",
        "Error": data.get('error_message') or "N/A",
        "Modo de Falla": data.get('failure_mode') or "Desconocido"
    }
    
    mensaje = f"""
    Se ha disparado una regla de <strong>Mitigación Activa</strong> debido a una transacción de alto riesgo.
    <br><br>
    El sistema ha registrado un evento con severidad {data['severity']} que ha resultado en un estado de <strong>{data['status']}</strong>.
    """
    
    html = _generar_html_template(
        "Alerta Proactiva FMECA",
        mensaje,
        detalles,
        color_primario=color,
        empresa_nombre="Sistema de Gestión de Riesgos"
    )
    
    success, err = await _enviar_email(recipient, subject, html, enterprise_id=data.get('enterprise_id'))
    if not success:
        logger.error(f"Fallo al enviar alerta de mitigación: {err}")

async def seed_default_rules():
    """Seed de reglas básicas de mitigación."""
    try:
        async with get_db_cursor() as cursor:
            # Borrar si existen para evitar duplicados en el seed
            await cursor.execute("TRUNCATE sys_risk_mitigation_rules")
            
            rules = [
                # Regla 1: Alerta por severidad extrema (9 o 10)
                (0, None, 9, 0, 'ALERT_EMAIL', 'marcelocperi@gmail.com', 1),
                # Regla 2: Log crítico por fallas de integridad de datos
                (0, 'DATA_INTEGRITY', 5, 0, 'LOG_CRITICAL', None, 1),
                # Regla 3: Alerta por fallas de seguridad
                (0, 'SECURITY_AUTH', 7, 0, 'ALERT_EMAIL', 'marcelocperi@gmail.com', 1)
            ]
            
            await cursor.executemany("""
                INSERT INTO sys_risk_mitigation_rules 
                (enterprise_id, failure_mode, min_severity, max_rpn, action_type, recipient_email, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rules)
            print("✅ Reglas de mitigación inicializadas.")
    except Exception as e:
        print(f"❌ Error al inicializar reglas: {e}")
