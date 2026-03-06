from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import NameOID

def generate_afip_request(name, org, cuit):
    # 1. Generar Clave Privada
    print("Generando clave privada...")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    
    # Guardar Clave Privada
    with open("afip_homo.key", "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    print("✅ Archivo 'afip_homo.key' generado.")

    # 2. Generar Pedido de Firma (CSR)
    print("Generando pedido de firma (CSR)...")
    subject = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "AR"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, org),
        x509.NameAttribute(NameOID.COMMON_NAME, name),
        x509.NameAttribute(NameOID.SERIAL_NUMBER, f"CUIT {cuit}"),
    ])
    
    csr = x509.CertificateSigningRequestBuilder().subject_name(
        subject
    ).sign(private_key, hashes.SHA256())

    # Guardar CSR
    with open("pedido.csr", "wb") as f:
        f.write(csr.public_bytes(serialization.Encoding.PEM))
    print("✅ Archivo 'pedido.csr' generado.")

if __name__ == "__main__":
    # Datos proporcionados por el usuario
    # Nombre (CN): "COLOSAL" (System Name)
    # Organización (O): "Marcelo Ceferino Peri" (Owner Name)
    # CUIT (serialNumber): "20171634432"
    generate_afip_request("COLOSAL", "Marcelo Ceferino Peri", "20171634432")
