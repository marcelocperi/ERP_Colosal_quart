import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Fallback credentials from email_service.py
SENDER_EMAIL = "marcelocperi@gmail.com"
SENDER_PASSWORD = "oahmgzdttkijeodi"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

def test_email_connection():
    print(f"Testing connection to {SMTP_SERVER}:{SMTP_PORT} with {SENDER_EMAIL}...")
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=15)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        print("Login SUCCESSFUL!")
        
        # Optionally send a very small test email to the same address
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = SENDER_EMAIL
        msg['Subject'] = "[TEST] Validacion de Servidor de Correo"
        msg.attach(MIMEText("Este es un mensaje de prueba para validar la configuracion SMTP.", "plain"))
        
        server.send_message(msg)
        print("Test email SENT to self successfully!")
        
        server.quit()
        return True
    except Exception as e:
        print(f"FAILED to send test email: {e}")
        return False

if __name__ == "__main__":
    test_email_connection()
