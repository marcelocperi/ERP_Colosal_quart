import os
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography import x509
from cryptography.x509.oid import NameOID
import datetime
import base64
import ipaddress

def generate_qz_keys(base_dir):
    """Generates an RSA private key and self-signed certificate for QZ Tray if they don't exist."""
    cert_path = os.path.join(base_dir, 'qz_cert.pem')
    key_path = os.path.join(base_dir, 'qz_private_key.pem')
    
    if os.path.exists(cert_path) and os.path.exists(key_path):
        return True, "Keys already exist."

    # Generate a private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    
    # Save the private key
    with open(key_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))

    # Generate a public certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"AR"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"CABA"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Buenos Aires"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Colosal Local"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
    ])
    
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.now(datetime.timezone.utc)
    ).not_valid_after(
        # Valid for 3650 days (10 years)
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650)
    ).add_extension(
        x509.SubjectAlternativeName([
            x509.DNSName(u"localhost"),
            x509.DNSName(u"colosal.local"),
            x509.IPAddress(ipaddress.IPv4Address(u"127.0.0.1"))
        ]),
        critical=False,
    ).sign(private_key, hashes.SHA256())
    
    # Save the certificate
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
        
    return True, "Keys generated successfully."

def sign_message(base_dir, message):
    """Signs a message with the private key to authenticate with QZ Tray."""
    key_path = os.path.join(base_dir, 'qz_private_key.pem')
    
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    
    signature = private_key.sign(
        message.encode('utf-8'),
        padding.PKCS1v15(),
        hashes.SHA512()
    )
    
    return base64.b64encode(signature).decode('utf-8')
