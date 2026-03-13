import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import datetime
import os, sys

# Database Access
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../multiMCP'))
from database import get_db_cursor

# Configuración de Correo Default (Fallback si la BD no tiene datos)
SENDER_EMAIL = ""
SENDER_PASSWORD = ""
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

async def get_enterprise_email_config(enterprise_id):
    """Obtiene credenciales de correo para una empresa especifica utilizando desencriptacion."""
    from cryptography.fernet import Fernet
    import os
    
    # Obtener la llave maestra de desencriptación
    key_path = os.path.join(os.path.dirname(__file__), '../../multiMCP', 'secret.key')
    cipher_suite = None
    if os.path.exists(key_path):
        with open(key_path, 'rb') as key_file:
            key = await key_file.read()
            cipher_suite = Fernet(key)

    try:
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT cuenta_mailing, mailing_password FROM sys_enterprises WHERE id = %s", (enterprise_id,))
            row = await cursor.fetchone()
            if row and row['cuenta_mailing'] and row['mailing_password'] and cipher_suite:
                try:
                    decrypted_pwd = cipher_suite.decrypt(row['mailing_password'].encode('utf-8')).decode('utf-8')
                    return {
                        'email': row['cuenta_mailing'],
                        'password': decrypted_pwd,
                        'server': SMTP_SERVER,
                        'port': SMTP_PORT
                    }
                except Exception as dec_err:
                    print(f"Failed to decrypt password for ent {enterprise_id}: {dec_err}")
    except Exception as e:
        print(f"Error fetching email config for ent {enterprise_id}: {e}")

    # Fallback to default
    return {
        'email': SENDER_EMAIL,
        'password': SENDER_PASSWORD,
        'server': SMTP_SERVER,
        'port': SMTP_PORT
    }

def _generar_html_template(titulo, mensaje_principal, detalles, color_primario="#6366f1", empresa_nombre="Colosal", logo_url=None):
    """Genera un template HTML responsive para los correos con branding de la empresa."""
    detalles_html = "".join([f"<li><strong>{k}:</strong> {v}</li>" for k, v in detalles.items()])
    
    # Construcción del área del logo: Priorizar logo de empresa si existe
    logo_display = f'<div class="logo-area">☀️📖🖋️</div>'
    if logo_url:
        # Asumiendo que logo_url es una ruta relativa como /static/uploads/logos/1.png
        # En un entorno real necesitamos la URL base completa (ej: https://midominio.com)
        # Por ahora lo incluimos tal cual, el servidor de correo o proxy debería manejarlo
        logo_display = f'<div class="logo-area"><img src="{logo_url}" alt="{empresa_nombre}" style="max-height: 80px; width: auto;"></div>'

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7fa; margin: 0; padding: 0; }}
            .container {{ max-width: 600px; margin: 20px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
            .header {{ background-color: {color_primario}; padding: 40px 20px; text-align: center; color: white; }}
            .logo-area {{ margin-bottom: 15px; }}
            .content {{ padding: 30px; line-height: 1.6; color: #333; }}
            .details {{ background-color: #f8fafc; border-radius: 8px; padding: 20px; margin-top: 20px; border: 1px solid #e2e8f0; }}
            .footer {{ background-color: #f1f5f9; padding: 20px; text-align: center; font-size: 12px; color: #64748b; }}
            ul {{ list-style: none; padding: 0; }}
            li {{ padding: 8px 0; border-bottom: 1px solid #edf2f7; }}
            li:last-child {{ border-bottom: none; }}
            .badge {{ display: inline-block; padding: 4px 12px; border-radius: 20px; background-color: {color_primario}; color: white; font-size: 14px; font-weight: bold; }}
            @media only screen and (max-width: 600px) {{
                .container {{ margin: 0; width: 100% !important; border-radius: 0; }}
                .content {{ padding: 20px; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                {logo_display}
                <h1 style="margin:0; font-size: 24px; letter-spacing: 2px; text-transform: uppercase;">{empresa_nombre}</h1>
                <p style="margin:5px 0 0 0; opacity: 0.8;">{titulo}</p>
            </div>
            <div class="content">
                <p>Hola <strong>{detalles.get('Usuario', 'Cliente')}</strong>,</p>
                <p>{mensaje_principal}</p>
                
                <div class="details">
                    <h3>Detalles del Registro:</h3>
                    <ul>
                        {detalles_html}
                    </ul>
                </div>
            </div>
            <div class="footer">
                <p>&copy; {datetime.datetime.now().year} {empresa_nombre} | Gestión Multi-Tenant</p>
                <p>Este es un correo automático enviado por el sistema de gestión corporativa.</p>
            </div>
        </div>
    </body>
    </html>
    """

async def _obtener_branding(enterprise_id):
    """Obtiene el nombre, logo y CUIT de la empresa de la BD."""
    from database import get_db_cursor
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT nombre, logo_path, cuit FROM sys_enterprises WHERE id = %s", (enterprise_id,))
            res = await cursor.fetchone()
            if res:
                return res['nombre'] or "Colosal", res['logo_path'], res['cuit']
    except Exception:
        pass
    return "Colosal", None, ""

async def _enviar_email(recipient_email, subject, html_content, attachments=None, enterprise_id=None):
    """
    Versión unificada y robusta para envío de correos.
    Soporta configuraciones por empresa y adjuntos (paths o tuplas).
    Retorna (True, None) si tiene éxito, o (False, error_msg) si falla.
    """
    email_config = await get_enterprise_email_config(enterprise_id)
    sender_email = email_config['email']
    sender_password = email_config['password']
    smtp_server = email_config['server']
    smtp_port = email_config['port']

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = recipient_email
    
    # Aplicar prefijo [COLOSAL] si no está presente
    final_subject = subject
    if not subject.startswith("[COLOSAL]"):
        final_subject = f"[COLOSAL] {subject}"
    msg["Subject"] = final_subject

    msg.attach(MIMEText(html_content, "html"))

    if attachments:
        for attachment in attachments:
            try:
                if isinstance(attachment, str):
                    # Es un path de archivo
                    if os.path.exists(attachment):
                        with open(attachment, "rb") as f:
                            part = MIMEApplication(await f.read(), Name=os.path.basename(attachment))
                        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment)}"'
                        msg.attach(part)
                elif isinstance(attachment, tuple) and len(attachment) == 2:
                    # Es una tupla (filename, content_bytes)
                    filename, content = attachment
                    part = MIMEApplication(content)
                    part.add_header('Content-Disposition', 'attachment', filename=filename)
                    msg.attach(part)
            except Exception as e:
                print(f"Error attaching {attachment}: {e}")

    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=15) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        return True, None
    except Exception as e:
        err_msg = str(e)
        print(f"Error sending email to {recipient_email}: {err_msg}")
        return False, err_msg


async def enviar_notificacion_prestamo(usuario_email, usuario_nombre, libro_nombre, isbn, fecha_dev, prestamo_id, enterprise_id):
    subject = f"Préstamo #{prestamo_id}"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    detalles = {
        "Usuario": usuario_nombre,
        "Libro": libro_nombre,
        "ISBN": isbn,
        "Fecha de Préstamo": datetime.date.today().strftime("%d/%m/%Y"),
        "Vencimiento": fecha_dev,
        "ID Préstamo": prestamo_id
    }
    
    html = _generar_html_template(
        "Préstamo Registrado", 
        "Tu solicitud de préstamo ha sido procesada con éxito. Recuerda devolver el ejemplar antes de la fecha de vencimiento.",
        detalles,
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_notificacion_devolucion(usuario_email, usuario_nombre, libro_datos, fecha_prestamo, prestamo_id, enterprise_id):
    subject = f"Devolución #{prestamo_id}"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    # Manejar si libro_datos es string (legacy) o dict
    titulo = libro_datos if isinstance(libro_datos, str) else libro_datos.get('titulo')
    autor = "" if isinstance(libro_datos, str) else libro_datos.get('autor', '')
    isbn = "" if isinstance(libro_datos, str) else libro_datos.get('isbn', '')

    detalles = {
        "Usuario": usuario_nombre,
        "Libro": titulo,
        "Autor": autor,
        "ISBN": isbn,
        "Fecha de Préstamo": fecha_prestamo,
        "Fecha de Devolución": datetime.datetime.now().strftime("%d/%m/%Y"),
        "Estado": "Devuelto con éxito"
    }
    
    # Limpiar campos vacíos
    detalles = {k: v for k, v in detalles.items() if v}

    html = _generar_html_template(
        "Devolución Registrada", 
        "Hemos recibido el libro correctamente. Esperamos que hayas disfrutado de la lectura y te invitamos a buscar tu próxima aventura en nuestro catálogo.",
        detalles,
        color_primario="#10b981",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_clave_temporal(usuario_email, usuario_nombre, clave_temporal, enterprise_id):
    subject = "Seguridad: Acceso Temporal al Sistema"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    detalles = {
        "Usuario": usuario_nombre,
        "Clave Temporal": f"<code style='background: #e2e8f0; padding: 2px 5px; border-radius: 4px;'>{clave_temporal}</code>",
        "Vencimiento": "24 Horas",
        "Acción Requerida": "Cambiar contraseña al ingresar"
    }
    
    mensaje = f"""
    Usted ha solicitado una clave de acceso temporal para su cuenta. <br><br>
    Por favor, ingrese al sistema con esta clave y proceda a cambiarla inmediatamente. 
    Esta clave caducará en <strong>24 horas</strong>.
    <br><br>
    <div style="background-color: #ebf8ff; border-left: 4px solid #4299e1; padding: 15px;">
        Si usted no solicitó este cambio, ignore este correo o contacte al administrador.
    </div>
    """
    
    html = _generar_html_template(
        "Clave Temporal Generada", 
        mensaje,
        detalles,
        color_primario="#3b82f6",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_link_recuperacion(usuario_email, usuario_nombre, link, enterprise_id):
    """
    Envía un enlace único para restablecer contraseña.
    """
    subject = "Recuperación de Acceso - Enlace Seguro"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    detalles = {
        "Usuario": usuario_nombre,
        "Vencimiento": "1 Hora",
        "Acción": "Restablecer Contraseña"
    }

    mensaje = f"""
    Hemos recibido una solicitud para restablecer la contraseña de su cuenta <strong>{usuario_nombre}</strong>.
    <br><br>
    Haga clic en el siguiente botón para crear una nueva contraseña:
    <br><br>
    <div style="text-align: center; margin: 30px 0;">
        <a href="{link}" style="background-color: #3b82f6; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">Restablecer Contraseña</a>
    </div>
    <br>
    <small>Si el botón no funciona, copie y pegue el siguiente enlace en su navegador:</small>
    <br>
    <code style="word-break: break-all; color: #64748b; font-size: 11px;">{link}</code>
    <br><br>
    <div style="background-color: #fffbeb; border-left: 4px solid #f59e0b; padding: 10px;">
        Si usted no solicitó este cambio, ignore este correo. Su contraseña actual permanecerá segura.
    </div>
    """

    html = _generar_html_template(
        "Recuperación de Contraseña",
        mensaje,
        detalles,
        color_primario="#3b82f6",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def alert_admin_multiples_intentos(admin_email, usuario_nombre, intentos):
    subject = f"ALERTA SEGURIDAD: Múltiples intentos de recuperación - {usuario_nombre}"
    ahora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    detalles = {
        "Usuario": usuario_nombre,
        "Intentos Detectados": intentos,
        "Fecha Reporte": ahora,
        "Estado": "BLOQUEO PREVENTIVO DE REGENERACIÓN"
    }
    
    mensaje = f"""
    Se ha detectado una actividad inusual para el usuario <strong>{usuario_nombre}</strong>. 
    Se han realizado {intentos} solicitudes de regeneración de clave en un corto período.
    <br><br>
    Se recomienda revisar el estado de la cuenta por posible intento de suplantación.
    """
    
    html = _generar_html_template(
        "Alerta de Seguridad Crítica", 
        mensaje,
        detalles,
        color_primario="#7f1d1d" # Rojo oscuro
    )

    # Nota: No pasamos enterprise_id aqui ya que es una alerta global de seguridad
    return await _enviar_email(admin_email, subject, html)

async def enviar_notificacion_cambio_password(usuario_email, usuario_nombre, admin_email=None, enterprise_id=None):
    subject = "Seguridad: Cambio de Contraseña Confirmado"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    ahora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    detalles = {
        "Usuario": usuario_nombre,
        "Fecha y Hora": ahora,
        "Estado": "Actualizada correctamente"
    }
    
    # Mensaje principal con advertencia
    mensaje = f"""
    Se ha detectado un cambio de clave para el usuario: <strong>{usuario_nombre}</strong>. <br><br>
    <div style="background-color: #fffbeb; border-left: 4px solid #f6e05e; padding: 15px; margin-top: 15px;">
        <strong>ADVERTENCIA DE SEGURIDAD:</strong> Si usted no realizó este cambio, por favor comuníquese de inmediato con el administrador del sistema para bloquear su cuenta.
    </div>
    """
    
    html = _generar_html_template(
        "Notificación de Seguridad", 
        mensaje,
        detalles,
        color_primario="#f43f5e", # Color rojo/rosa para seguridad
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    res_user = (True, None)
    if usuario_email:
        res_user = await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)
    
    res_admin = (True, None)
    if admin_email and (not usuario_email or admin_email != usuario_email):
        subject_admin = f"AVISO SEGURIDAD: Cambio de clave - {usuario_nombre}"
        res_admin = await _enviar_email(admin_email, subject_admin, html, enterprise_id=enterprise_id)
        
    return res_user if usuario_email else res_admin

async def enviar_confirmacion_reserva(usuario_email, usuario_nombre, libro_datos, fecha_estimada, enterprise_id):
    subject = "Reserva Confirmada"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    titulo = libro_datos.get('titulo') if isinstance(libro_datos, dict) else str(libro_datos)
    autor = libro_datos.get('autor', 'Desconocido') if isinstance(libro_datos, dict) else ''
    isbn = libro_datos.get('isbn', '-') if isinstance(libro_datos, dict) else ''

    detalles = {
        "Usuario": usuario_nombre,
        "Libro": titulo,
        "Autor": autor,
        "ISBN": isbn,
        "Fecha Estimada Disponible": fecha_estimada,
        "Estado": "En espera de devolución"
    }
    # Clean empty values
    detalles = {k: v for k, v in detalles.items() if v}
    
    msg = """
    Tu reserva ha sido registrada correctamente. Te notificaremos vía email tan pronto como el ejemplar esté disponible para retirar.
    <br>Esta reserva tiene una validez de <strong>48 horas</strong> a partir del momento en que se le notifique la disponibilidad.
    """
    
    html = _generar_html_template(
        "Reserva Registrada", 
        msg,
        detalles,
        color_primario="#f59e0b", # Naranja/Amber
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_disponibilidad_reserva(usuario_email, usuario_nombre, libro_datos, fecha_limite, enterprise_id):
    subject = "¡Tu libro reservado está disponible!"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    titulo = libro_datos.get('titulo') if isinstance(libro_datos, dict) else str(libro_datos)
    autor = libro_datos.get('autor', '') if isinstance(libro_datos, dict) else ''
    
    detalles = {
        "Usuario": usuario_nombre,
        "Libro": titulo,
        "Autor": autor,
        "Disponible Desde": datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
        "Retirar Antes De": fecha_limite,
        "Acción": "Retirar en biblioteca"
    }
    detalles = {k: v for k, v in detalles.items() if v}

    msg = f"""
    El libro <strong>{titulo}</strong> que reservaste ya ha sido devuelto y está disponible para ti.
    <br><br>
    Tienes hasta el <strong>{fecha_limite}</strong> (48 horas) para retirarlo. Pasado este tiempo, la reserva expirará y el libro estará disponible para otros usuarios.
    """
    
    html = _generar_html_template(
        "Reserva Disponible", 
        msg,
        detalles,
        color_primario="#10b981", # Verde
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_cancelacion_reserva(usuario_email, usuario_nombre, libro_datos, motivo, enterprise_id):
    subject = "Actualización de Reserva Cancelada"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    titulo = libro_datos.get('titulo') if isinstance(libro_datos, dict) else str(libro_datos)
    autor = libro_datos.get('autor', '') if isinstance(libro_datos, dict) else ''

    detalles = {
        "Usuario": usuario_nombre,
        "Libro": titulo,
        "Autor": autor,
        "Estado": "Cancelada",
        "Motivo": motivo,
        "Fecha": datetime.datetime.now().strftime("%d/%m/%Y")
    }
    detalles = {k: v for k, v in detalles.items() if v}
    
    msg = f"""
    Te informamos que tu reserva para el libro <strong>{titulo}</strong> ha sido CANCELADA.
    <br><br>
    <strong>Motivo:</strong> {motivo}
    <br><br>
    Si crees que esto es un error, por favor contacta con la biblioteca.
    """
    
    html = _generar_html_template(
        "Reserva Cancelada", 
        msg,
        detalles,
        color_primario="#ef4444", # Rojo
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

def validar_estado_correo(email):
    """
    Realiza una validación del estado del correo.
    """
    import re
    # 1. Formato
    regex = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
    if not re.match(regex, email, re.I):
        return False, "Formato de correo inválido."
    
    # 2. Dominios comunes (Simulación de validación de quota/existencia)
    # En un entorno real se usaría una API como Hunter.io o ZeroBounce
    # Para este MVP, bloqueamos dominios temporales conocidos
    blocked_domains = ['tempmail.com', 'throwawaymail.com', '10minutemail.com']
    domain = email.split('@')[1].lower()
    if domain in blocked_domains:
        return False, "No se permiten correos temporales."
        
    return True, "OK"



async def enviar_notificacion_retencion(destinatario_email, sujeto_nombre, certificado_nro, tipo, importe, comprobantes, enterprise_id):
    """
    Envía el certificado de retención con un resumen responsive de los comprobantes que lo originan.
    """
    empresa_nombre, logo_path, empresa_cuit = await _obtener_branding(enterprise_id)
    
    # Nuevo formato de subject: CUIT_PERIODO_MM_YYYY_FEC_EMIS_DDMMYYYYHHMMSS
    now = datetime.datetime.now()
    periodo = now.strftime("%m_%Y")
    fec_emis = now.strftime("%d%m%Y%H%M%S")
    cuit_clean = str(empresa_cuit or "00000000000").replace("-", "").replace(".", "").replace(" ", "")
    
    subject = f"{cuit_clean}_{periodo}_{fec_emis}"
    
    # Construir tabla de comprobantes para el cuerpo del mail
    filas_html = ""
    total_base = 0
    for c in comprobantes:
        importe_c = float(c.get('importe_pagado', 0) or c.get('importe_total', 0))
        total_base += importe_c
        filas_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{c.get('fecha_emision')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{c.get('tipo_nombre')} {c.get('punto_venta')}-{c.get('numero')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee; text-align: right;">$ {importe_c:,.2f}</td>
            </tr>
        """

    mensaje = f"""
    Se adjunta el <strong>Certificado de Retención de {tipo}</strong> correspondiente al pago realizado en el día de la fecha.
    <br><br>
    <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 15px;">
        <h4 style="margin-top: 0; color: #1e293b;">Resumen de la Transacción</h4>
        <table style="width: 100%; font-size: 13px; border-collapse: collapse;">
            <thead>
                <tr style="background: #f1f5f9;">
                    <th style="padding: 8px; text-align: left;">Fecha</th>
                    <th style="padding: 8px; text-align: left;">Comprobante</th>
                    <th style="padding: 8px; text-align: right;">Monto</th>
                </tr>
            </thead>
            <tbody>
                {filas_html}
            </tbody>
            <tfoot>
                <tr style="font-weight: bold;">
                    <td colspan="2" style="padding: 8px; text-align: right;">Base Imponible:</td>
                    <td style="padding: 8px; text-align: right;">$ {total_base:,.2f}</td>
                </tr>
                <tr style="font-weight: bold; color: #e11d48;">
                    <td colspan="2" style="padding: 8px; text-align: right;">Monto Retenido:</td>
                    <td style="padding: 8px; text-align: right;">$ {float(importe):,.2f}</td>
                </tr>
            </tfoot>
        </table>
    </div>
    """

    detalles = {
        "Usuario": sujeto_nombre,
        "Certificado Nro": certificado_nro,
        "Impuesto": tipo,
        "Monto": f"$ {float(importe):,.2f}"
    }

    html = _generar_html_template(
        "Emisión de Certificado Fiscal",
        mensaje,
        detalles,
        color_primario="#1e293b",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    # Nota: Los adjuntos se manejaran desde el llamador para evitar dependencias circulares de PDF en este service
    return subject, html

async def enviar_notificacion_incidente(usuario_email, usuario_nombre, req_id, status, nota, history, enterprise_id):
    subject = f"Actualización de Incidente #{req_id}"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    estado_labels = {
        'OPEN': 'Abierto',
        'IN_PROGRESS': 'En Revisión / Atendimiento',
        'RESOLVED': 'Resuelto'
    }
    
    status_label = estado_labels.get(status, status)
    color = "#3b82f6"
    if status == 'RESOLVED':
        color = "#10b981"
    elif status == 'OPEN':
        color = "#ef4444"
        
    hist_html = ""
    if history:
        for itm in reversed(history[-5:]): # ultimos 5
            hist_html += f"<div style='background:#f8fafc; padding:10px; margin-bottom:5px; border-left: 3px solid {color}'><small style='color:#64748b'>{itm.get('date')} - {itm.get('user')}</small><br>{itm.get('note')}</div>"

    mensaje = f"""
    Estimado/a <strong>{usuario_nombre}</strong>, el incidente reportado bajo el ID #{req_id} ha sido actualizado.
    <br><br>
    <strong>Nuevo Estado del Incidente:</strong> <span style="background:{color}; color:white; padding: 2px 6px; border-radius: 4px;">{status_label}</span>
    <br><br>
    <strong>Último mensaje recibido:</strong><br>
    <i>"{nota if nota else '(Solo cambio de estado)'}"</i>
    <br><br>
    <strong>Historial Reciente:</strong><br>
    {hist_html if hist_html else 'Sin historial previo.'}
    """

    detalles = {
        "Incidente ID": f"#{req_id}",
        "Estado Actual": status_label,
        "Fecha de Actualización": datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    }

    html = _generar_html_template(
        "Actualización de Soporte",
        mensaje,
        detalles,
        color_primario=color,
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_notificacion_percepcion(destinatario_email, sujeto_nombre, factura_nro, importe_percepcion, enterprise_id):
    """
    Envía la notificación de percepción incluida en una factura de venta.
    """
    empresa_nombre, logo_path, empresa_cuit = await _obtener_branding(enterprise_id)
    
    # Aplicar mismo naming convention para tax docs
    now = datetime.datetime.now()
    periodo = now.strftime("%m_%Y")
    fec_emis = now.strftime("%d%m%Y%H%M%S")
    cuit_clean = str(empresa_cuit or "00000000000").replace("-", "").replace(".", "").replace(" ", "")
    
    subject = f"{cuit_clean}_{periodo}_{fec_emis}"
    
    mensaje = f"""
    Le informamos que se ha emitido la Factura <strong>{factura_nro}</strong> la cual incluye percepciones impositivas por un total de <strong>$ {float(importe_percepcion):,.2f}</strong>.
    <br><br>
    Este importe ha sido oportunamente informado a los organismos recaudadores correspondientes y puede ser computado como pago a cuenta de sus obligaciones fiscales.
    """

    detalles = {
        "Usuario": sujeto_nombre,
        "Factura Nro": factura_nro,
        "Monto Percepción": f"$ {float(importe_percepcion):,.2f}",
        "Fecha": datetime.date.today().strftime("%d/%m/%Y")
    }

    html = _generar_html_template(
        "Percepción Impositiva Registrada",
        mensaje,
        detalles,
        color_primario="#4338ca",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return subject, html
async def enviar_solicitud_devolucion(usuario_email, usuario_nombre, solicitud_id, factura_nro, items, enterprise_id):
    subject = f"Solicitud de Devolución #{solicitud_id} recibida"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    filas_html = ""
    for item in items:
        filas_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{item['nombre']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee; text-align: center;">{item['cantidad']}</td>
            </tr>
        """

    mensaje = f"""
    Estimado/a <strong>{usuario_nombre}</strong>, <br><br>
    Hemos recibido su solicitud de devolución correspondiente a la Factura <strong>{factura_nro}</strong>.
    <br><br>
    Le informamos que la emisión de la Nota de Crédito queda <strong>supeditada a la recepción física de la mercadería</strong> en nuestros depósitos. 
    Una vez recibida, la misma será evaluada por nuestro equipo técnico para proceder con la aprobación final del comprobante.
    <br><br>
    <strong>Por favor, proceda con el envío de los siguientes ítems:</strong>
    <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
        <thead>
            <tr style="background: #f1f5f9;">
                <th style="padding: 8px; text-align: left;">Artículo</th>
                <th style="padding: 8px; text-align: center;">Cantidad</th>
            </tr>
        </thead>
        <tbody>
            {filas_html}
        </tbody>
    </table>
    <br>
    Quedamos a su disposición por cualquier consulta adicional.
    """

    detalles = {
        "Solicitud ID": f"#{solicitud_id}",
        "Factura Origen": factura_nro,
        "Estado": "Pendiente de Recepción",
        "Fecha": datetime.date.today().strftime("%d/%m/%Y")
    }

    html = _generar_html_template(
        "Solicitud de Devolución",
        mensaje,
        detalles,
        color_primario="#6366f1",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_alerta_demora(usuario_email, usuario_nombre, despacho_nro, buque, fecha_arribo, dias_restantes, costo_diario, enterprise_id):
    """Notifica el vencimiento próximo de los días libres de puerto."""
    subject = f"ALERTA LOGÍSTICA: Vencimiento de Libres - Despacho {despacho_nro}"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    
    color = "#f59e0b" # Naranja
    if dias_restantes <= 0:
        color = "#ef4444" # Rojo si ya venció

    detalles = {
        "Usuario": usuario_nombre,
        "Despacho": despacho_nro,
        "Buque": buque,
        "Arribo Real": fecha_arribo,
        "Días Libres Restantes": f"<strong>{dias_restantes} días</strong>",
        "Costo Demora Diaria": f"U$S {costo_diario:,.2f}"
    }

    mensaje = f"""
    Le informamos que el plazo de <strong>días libres de puerto</strong> para la carga del buque <strong>{buque}</strong> está próximo a vencer.
    <br><br>
    Actualmente restan <strong>{dias_restantes} días</strong> para devolver el contenedor sin incurrir en costos adicionales.
    Evite cargos por demora gestionando la devolución antes del plazo estipulado.
    """
    
    if dias_restantes <= 0:
        mensaje = f"""
        <strong>¡ATENCIÓN!</strong> El plazo de días libres de puerto para la carga del buque <strong>{buque}</strong> ha EXCEDIDO.
        <br><br>
        Se están generando costos de demora de <strong>U$S {costo_diario:,.2f} por día</strong>. 
        Por favor, agilice la devolución del contenedor lo antes posible.
        """

    html = _generar_html_template(
        "Alerta de Demoras en Puerto",
        mensaje,
        detalles,
        color_primario=color,
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)

async def enviar_notificacion_propuesta_precios(usuario_email, usuario_nombre, lista_nombre, count, enterprise_id):
    subject = f"PENDIENTE: Propuesta de Precios - {lista_nombre}"
    empresa_nombre, logo_path, _ = await _obtener_branding(enterprise_id)
    detalles = {
        "Usuario": usuario_nombre,
        "Lista de Precios": lista_nombre,
        "Artículos Alcanzados": count,
        "Acción Requerida": "Revisión y Aprobación / Rechazo",
        "Prioridad": "Alta"
    }
    
    msg = f"""
    Se ha generado una nueva <strong>propuesta de precios</strong> para la lista <strong>{lista_nombre}</strong>.
    <br><br>
    Un total de <strong>{count} artículos</strong> han sido recalculados según las reglas vigentes y esperan su aprobación técnica 
    desde el rol de <strong>Cost Accounting</strong>.
    <br><br>
    Por favor, ingrese al módulo de Precios para validar línea a línea o realizar una aprobación masiva.
    """
    
    html = _generar_html_template(
        "Propuesta de Costeo Sugerido", 
        msg,
        detalles,
        color_primario="#4f46e5",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(usuario_email, subject, html, enterprise_id=enterprise_id)


async def enviar_ticket_embarque(destinatario_email, destinatario_nombre, comprobante_data, enterprise_id):
    """
    Envía el 'Ticket de Embarque' (Factura Electrónica finalizada con CAE) al pasajero.
    Se llama luego de obtener el CAE exitosamente, ya sea en tiempo real o tras reintentos.
    """
    empresa_nombre, logo_path, empresa_cuit = await _obtener_branding(enterprise_id)
    
    tipo = comprobante_data.get('tipo_nombre', 'Comprobante')
    numero = f"{str(comprobante_data.get('punto_venta', 1)).zfill(5)}-{str(comprobante_data.get('numero', 0)).zfill(8)}"
    cae = comprobante_data.get('cae', 'N/D')
    cae_vto = comprobante_data.get('cae_vto', 'N/D')
    total = comprobante_data.get('total', 0)
    fecha = comprobante_data.get('fecha_emision', datetime.date.today().strftime('%d/%m/%Y'))

    subject = f"Tu {tipo} N° {numero} - {empresa_nombre}"

    detalles = {
        "Usuario": destinatario_nombre,
        "Comprobante": f"{tipo} N° {numero}",
        "Fecha de Emisión": str(fecha),
        "Total": f"$ {float(total):,.2f}",
        "CAE": cae,
        "Vto. CAE": str(cae_vto),
    }

    mensaje = f"""
    Estimado/a <strong>{destinatario_nombre}</strong>,<br><br>
    Adjuntamos su comprobante fiscal debidamente autorizado por AFIP/ARCA.<br><br>
    El Código de Autorización de Emisión (CAE) <strong>{cae}</strong> 
    garantiza la validez legal de este documento hasta el <strong>{cae_vto}</strong>.<br><br>
    <div style="background-color: #f0fdf4; border-left: 4px solid #22c55e; padding: 12px; border-radius: 4px;">
        ✅ Este comprobante es válido y ha sido informado a AFIP/ARCA correctamente.
    </div>
    """

    html = _generar_html_template(
        "Comprobante Fiscal Autorizado",
        mensaje,
        detalles,
        color_primario="#4f46e5",
        empresa_nombre=empresa_nombre,
        logo_url=logo_path
    )

    return await _enviar_email(destinatario_email, subject, html, enterprise_id=enterprise_id)


async def procesar_cae_pendientes(enterprise_id=None):
    """
    Job de Reintento: El Nabucodonosor revisa la cola de CAEs pendientes.
    Para cada uno, reintenta obtener el CAE y si lo logra, envía el Ticket de Embarque al pasajero.
    Diseñado para ejecutarse desde un scheduler (APScheduler, cron, etc.).
    """
    from services.afip_service import AfipService

    filtro_ent = "AND enterprise_id = %s" if enterprise_id else ""
    params = (enterprise_id,) if enterprise_id else ()

    try:
        async with get_db_cursor(dictionary=True) as cur:
            await cur.execute(f"""
                SELECT p.id, p.enterprise_id, p.comprobante_id, p.intentos
                FROM fin_cae_pendientes p
                WHERE p.estado = 'PENDIENTE'
                AND p.proximo_intento <= NOW()
                AND p.intentos < 10
                {filtro_ent}
                ORDER BY p.creado_en ASC
                LIMIT 20
            """, params)
            pendientes = await cur.fetchall()
    except Exception as e:
        print(f"[RETRY-CAE] Error leyendo cola: {e}")
        return {"procesados": 0, "errores": 1}

    resultados = {"procesados": 0, "errores": 0, "enviados_email": 0}

    for item in pendientes:
        eid = item['enterprise_id']
        cbte_id = item['comprobante_id']
        print(f"[RETRY-CAE] Reintentando CAE para comprobante {cbte_id} (empresa {eid})...")

        try:
            # Reintento real contra AFIP
            resultado = await AfipService._ejecutar_solicitud_cae(
                None, eid, cbte_id,
                await AfipService.get_afip_config(eid)
            )

            if resultado.get('success'):
                # ¡CAE obtenido! Actualizar estado en cola
                async with get_db_cursor(dictionary=True) as cur:
                    await cur.execute("""
                        UPDATE fin_cae_pendientes 
                        SET estado = 'PROCESADO', ultimo_intento = NOW(), ultimo_error = NULL
                        WHERE id = %s
                    """, (item['id'],))

                # Obtener datos del comprobante para enviar el mail
                try:
                    async with get_db_cursor(dictionary=True) as cur:
                        await cur.execute("""
                            SELECT c.*, t.email as cliente_email, t.nombre as cliente_nombre,
                                   t.cuit as cliente_cuit, tc.nombre as tipo_nombre
                            FROM erp_comprobantes c
                            JOIN erp_terceros t ON c.tercero_id = t.id
                            LEFT JOIN fin_tipos_comprobantes tc ON c.tipo_comprobante = tc.codigo
                            WHERE c.id = %s AND c.enterprise_id = %s
                        """, (cbte_id, eid))
                        cbte = await cur.fetchone()

                    if cbte and cbte.get('cliente_email'):
                        cbte['cae'] = resultado.get('cae')
                        cbte['cae_vto'] = resultado.get('cae_vto')
                        cbte['total'] = float(cbte.get('importe_total', 0))
                        cbte['numero'] = cbte.get('numero_comprobante')
                        
                        ok, _ = await enviar_ticket_embarque(
                            cbte['cliente_email'],
                            cbte['cliente_nombre'],
                            cbte,
                            eid
                        )
                        if ok:
                            resultados['enviados_email'] += 1
                            print(f"[RETRY-CAE] ✅ Ticket de embarque enviado a {cbte['cliente_email']}")
                except Exception as mail_err:
                    print(f"[RETRY-CAE] Error enviando email post-CAE: {mail_err}")

                resultados['procesados'] += 1
            else:
                # Aún falla: incrementar intentos y reprogramar
                prox = "DATE_ADD(NOW(), INTERVAL 30 MINUTE)"
                async with get_db_cursor(dictionary=True) as cur:
                    await cur.execute(f"""
                        UPDATE fin_cae_pendientes 
                        SET intentos = intentos + 1, ultimo_intento = NOW(),
                            proximo_intento = {prox},
                            ultimo_error = %s
                        WHERE id = %s
                    """, (str(resultado.get('error', ''))[:500], item['id']))
                resultados['errores'] += 1

        except Exception as e:
            print(f"[RETRY-CAE] Error en retry de comprobante {cbte_id}: {e}")
            resultados['errores'] += 1

    print(f"[RETRY-CAE] Resultado: {resultados}")
    return resultados
