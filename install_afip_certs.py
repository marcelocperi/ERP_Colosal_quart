from database import get_db_cursor
import os

def install_certificates():
    # El certificado que proporcionó el usuario
    crt_text = """-----BEGIN CERTIFICATE-----
MIIDSzCCAjOgAwIBAgIIRbbQGUHnBKEwDQYJKoZIhvcNAQENBQAwODEaMBgGA1UEAwwRQ29tcHV0
YWRvcmVzIFRlc3QxDTALBgNVBAoMBEFGSVAxCzAJBgNVBAYTAkFSMB4XDTI2MDIyMDE2MjMzMVoX
DTI4MDIyMDE2MjMzMVowMTEUMBIGA1UEAwwLVEVTVENPTE9TQUwxGTAXBgNVBAUTEENVSVQgMjAx
NzE2MzQ0MzIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC4p2scO76X4zR1wYK4wCAM
yaPq+5L1ottnzSceONVOJL7AAmzbd1g5/OjBfwdZtzNQiaXd46PTUzFk0w94exc1VTAK2KIPMk+/
LnXnRu6lAWsQt1SDemw8NU5fRu1ThAx/i8VVnkcDtn0YZ4Yi6J7iiELEVvTG16gmuyUvynE9u33f
UPQRaPo8MHrVDWuW8lrHw7N9ikPPJhsR/MRf4DOGUfdE14kES3xBBkWys2FZN4bADwpcCVzPNaX3
cG6vhzIXSa8Yg2PE1jMyC+YUAdCXq99xv5yeXCpebAiP2lcsYU/eGwKIUuDvyhyTcdkc00DA2VdO
sxY+Sn7Ql9OtzQdXAgMBAAGjYDBeMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAUs7LT//3put7e
ja8RIZzWIH3yT28wHQYDVR0OBBYEFM4ta6YtSHqabKarSFGD0WlKyBNkMA4GA1UdDwEB/wQEAwIF
4DANBgkqhkiG9w0BAQ0FAAOCAQEAqCM2wNFXa+jlGMggosQ0ZRIT6gI5mp7yg8Nyd4UbhG8N/SNa
KY5KOGXdvGY7HaTj8gksCl5RmtbFPDt0M7eAXUVMEkYzXrK43bd45gXLZUg+YnCCxcWOVjmHoZBK
rFRBHLIV1JUQkZidWr8THJyL3Awbq0k6ox5zF3XxxUOpuwobiNf7ZumR8qwKOMpIePvJ3xFs4Gui
wT/mfKzC2kWMYPdWcJ9Cj+9eXIGgqCYlPZ6IPsLKUqgyOH+i5gC1ldj1zoNDrofN+9VYG0z0TKRU
2Yf249tRgRS7B4zDfqDgHtlj8bAYmIPV2IbyNXo4mfzdFal/a+n0UmN0uciA4mSlgA==
-----END CERTIFICATE-----"""

    # Leer la clave privada que generamos antes
    key_path = "afip_homo.key"
    if not os.path.exists(key_path):
        print("ERROR: No se encontró el archivo afip_homo.key")
        return

    with open(key_path, "r") as f:
        key_text = f.read()

    with get_db_cursor() as cursor:
        print("Updating Enterprise 0 AFIP configuration...")
        cursor.execute("""
            UPDATE sys_enterprises 
            SET afip_crt = %s, 
                afip_key = %s, 
                afip_entorno = 'testing' 
            WHERE id = 0
        """, (crt_text, key_text))
    
    print("\n✅ CONFIGURATION UPDATED FOR ENTERPRISE 0.")
    print("👉 Environment set to: TESTING")
    print("👉 Cert and Key installed.")

if __name__ == "__main__":
    install_certificates()
