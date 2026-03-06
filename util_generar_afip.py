
import os
import sys
import subprocess

def install_and_import(package):
    try:
        __import__(package)
    except ImportError:
        print(f"📦 Instalando {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def generar_archivos_afip(cuit, organizacion="Empresa", nombre_alias="Facturacion"):
    install_and_import('cryptography')
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    print(f"🚀 Generando Clave Privada y Pedido para CUIT: {cuit}...")

    # 1. Generar Clave Privada
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    # Guardar Clave Privada
    with open("privada.key", "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))

    # 2. Generar CSR (Pedido)
    csr = x509.CertificateSigningRequestBuilder().subject_name(x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"AR"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, organizacion),
        x509.NameAttribute(NameOID.SERIAL_NUMBER, f"CUIT {cuit}"),
        x509.NameAttribute(NameOID.COMMON_NAME, nombre_alias),
    ])).sign(key, hashes.SHA256())

    # Guardar Pedido (CSR)
    with open("pedido.csr", "wb") as f:
        f.write(csr.public_bytes(serialization.Encoding.PEM))

    print("\n✅ ¡Archivos generados con éxito!")
    print(f"🔑 Clave Privada: {os.path.abspath('privada.key')}")
    print(f"📄 Pedido (CSR):  {os.path.abspath('pedido.csr')}")
    print("\nPROXIMOS PASOS:")
    print("1. Suba el contenido de 'pedido.csr' a la web de AFIP (Administración de Certificados Digitales).")
    print("2. Pegue el contenido de 'privada.key' en la Configuración Fiscal del ERP.")

if __name__ == "__main__":
    cuit_real = "20171634432" # El que usaste en el ejemplo
    generar_archivos_afip(cuit_real)
