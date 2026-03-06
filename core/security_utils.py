"""
🔒 Utilidades de seguridad para sanitización y validación
"""

import re
import os
from pathlib import Path
from typing import Tuple, Optional

# Configuración de seguridad
MAX_FILENAME_LENGTH = 255
MAX_FILE_SIZE_MB = int(os.environ.get("MAX_FILE_SIZE_MB", "50"))
ALLOWED_EXTENSIONS = os.environ.get("ALLOWED_EXTENSIONS", "pdf,epub,mobi").split(",")

# File signatures (magic numbers) para validación
FILE_SIGNATURES = {
    'pdf': b'%PDF',
    'epub': b'PK\x03\x04',  # ZIP-based
    'mobi': b'BOOKMOBI',
}

def sanitize_filename(filename: str) -> str:
    """
    Sanitiza nombre de archivo para prevenir ataques.
    
    Protege contra:
    - Path traversal (../, ..\)
    - Null bytes
    - Caracteres especiales peligrosos
    - Nombres de archivo reservados (Windows)
    
    Args:
        filename: Nombre de archivo a sanitizar
        
    Returns:
        Nombre de archivo seguro
        
    Examples:
        >>> sanitize_filename("../../etc/passwd")
        'passwd'
        >>> sanitize_filename("file\x00.pdf")
        'file.pdf'
        >>> sanitize_filename("CON.txt")  # Windows reserved
        'CON_safe.txt'
    """
    if not filename:
        return "unnamed_file"
    
    # Remover null bytes
    filename = filename.replace('\x00', '')
    
    # Remover path traversal
    filename = os.path.basename(filename)
    
    # Remover caracteres peligrosos (whitelist approach)
    # Solo permitir: letras, números, guión, guión bajo, punto
    filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
    
    # Prevenir nombres reservados de Windows
    RESERVED_NAMES = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
                      'LPT1', 'LPT2', 'LPT3'}
    if '.' in filename:
        name_part, ext_part = filename.rsplit('.', 1)
        if name_part.upper() in RESERVED_NAMES:
            filename = f"{name_part}_safe.{ext_part}"
    else:
        if filename.upper() in RESERVED_NAMES:
            filename = f"{filename}_safe"
    
    # Limitar longitud
    if len(filename) > MAX_FILENAME_LENGTH:
        name, ext = os.path.splitext(filename)
        filename = name[:MAX_FILENAME_LENGTH - len(ext)] + ext
    
    # Si quedó vacío, usar default
    if not filename or filename == '.':
        filename = "unnamed_file"
    
    return filename


def validate_file_extension(filename: str) -> bool:
    """
    Valida que la extensión del archivo esté permitida.
    
    Args:
        filename: Nombre del archivo
        
    Returns:
        True si la extensión es válida
    """
    if not filename:
        return False
    
    ext = Path(filename).suffix.lower().lstrip('.')
    return ext in ALLOWED_EXTENSIONS


def validate_file_signature(content: bytes, expected_type: str) -> bool:
    """
    Valida que el contenido del archivo coincida con su tipo declarado.
    Previene ataques donde archivos maliciosos se disfrazan con extensión falsa.
    
    Args:
        content: Contenido binario del archivo
        expected_type: Tipo esperado ('pdf', 'epub', 'mobi')
        
    Returns:
        True si el signature coincide
        
    Examples:
        >>> content = b'%PDF-1.4...'
        >>> validate_file_signature(content, 'pdf')
        True
        >>> validate_file_signature(b'<html>...', 'pdf')  # Malicious
        False
    """
    if expected_type not in FILE_SIGNATURES:
        return False
    
    signature = FILE_SIGNATURES[expected_type]
    return content.startswith(signature)


def validate_content_length(headers: dict, max_size_mb: int = MAX_FILE_SIZE_MB) -> Tuple[bool, Optional[int]]:
    """
    Valida el tamaño del contenido antes de descargar.
    Previene DoS por descarga de archivos gigantes.
    
    Args:
        headers: Headers HTTP del response
        max_size_mb: Tamaño máximo permitido en MB
        
    Returns:
        (es_válido, tamaño_en_bytes)
        
    Examples:
        >>> headers = {'Content-Length': '1048576'}  # 1MB
        >>> validate_content_length(headers, max_size_mb=50)
        (True, 1048576)
        >>> headers = {'Content-Length': '104857600'}  # 100MB
        >>> validate_content_length(headers, max_size_mb=50)
        (False, 104857600)
    """
    content_length = headers.get('Content-Length') or headers.get('content-length')
    
    if not content_length:
        # Si no hay Content-Length, rechazar por seguridad
        return False, None
    
    try:
        size_bytes = int(content_length)
        max_bytes = max_size_mb * 1024 * 1024
        
        return size_bytes <= max_bytes, size_bytes
    except (ValueError, TypeError):
        return False, None


def sanitize_url(url: str) -> str:
    """
    Sanitiza URL para logging seguro.
    Remueve query params que puedan contener tokens/keys.
    
    Args:
        url: URL original
        
    Returns:
        URL sanitizada para logs
        
    Examples:
        >>> sanitize_url("https://api.com/book?token=secret123")
        'https://api.com/book?token=***'
    """
    if not url:
        return ""
    
    # Remover tokens/keys de query params
    sensitive_params = ['token', 'key', 'password', 'secret', 'apikey', 'api_key']
    
    for param in sensitive_params:
        url = re.sub(f'{param}=[^&]+', f'{param}=***', url, flags=re.IGNORECASE)
    
    return url


# Decorador para validación de inputs
def validate_input(input_type: str = 'filename'):
    """
    Decorador para validar inputs automáticamente.
    
    Usage:
        @validate_input('filename')
        def process_file(filename: str):
            # filename ya está sanitizado
            pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            if input_type == 'filename' and args:
                args = list(args)
                args[0] = sanitize_filename(args[0])
            return func(*args, **kwargs)
        return wrapper
    return decorator


if __name__ == "__main__":
    # Tests básicos
    print("🧪 Testing security utilities...")
    
    # Test 1: Path traversal
    assert sanitize_filename("../../etc/passwd") == "passwd", "Path traversal failed"
    print("✅ Path traversal protection works")
    
    # Test 2: Null bytes
    assert sanitize_filename("file\x00.pdf") == "file.pdf", "Null byte failed"
    print("✅ Null byte protection works")
    
    # Test 3: Reserved names
    assert sanitize_filename("CON.txt") == "CON_safe.txt", "Reserved name failed"
    print("✅ Reserved name protection works")
    
    # Test 4: File signature
    pdf_content = b'%PDF-1.4\n%....'
    assert validate_file_signature(pdf_content, 'pdf'), "PDF signature failed"
    print("✅ File signature validation works")
    
    # Test 5: Content length
    headers = {'Content-Length': '1048576'}  # 1MB
    valid, size = validate_content_length(headers, max_size_mb=50)
    assert valid and size == 1048576, "Content length failed"
    print("✅ Content length validation works")
    
    print("\n🎉 All security tests passed!")
