
import pytest
import sys
import os

# Ajustar path al root (multiMCP)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.security_utils import (
    sanitize_filename, 
    validate_file_extension, 
    validate_file_signature, 
    validate_content_length
)

class TestSecurityUtils:
    
    # 1. Tests de Sanitización (Path Traversal + Null Bytes)
    def test_sanitize_filename_basic(self):
        assert sanitize_filename("normal_file.pdf") == "normal_file.pdf"
        assert sanitize_filename("File with spaces.pdf") == "File_with_spaces.pdf"

    def test_sanitize_filename_path_traversal(self):
        assert sanitize_filename("../../etc/passwd") == "passwd"
        assert sanitize_filename("..\\windows\\system32\\cmd.exe") == "cmd.exe"

    def test_sanitize_filename_null_bytes(self):
        assert sanitize_filename("malicious\x00file.php") == "maliciousfile.php"

    def test_sanitize_filename_reserved_windows(self):
        assert sanitize_filename("CON.txt") == "CON_safe.txt"
        assert sanitize_filename("aux.py") == "aux_safe.py"
        assert sanitize_filename("NUL") == "NUL_safe"

    def test_sanitize_filename_empty(self):
        assert sanitize_filename("") == "unnamed_file"
        assert sanitize_filename(".") == "unnamed_file"

    # 2. Tests de Validación de Extensión
    def test_validate_extension_allowed(self):
        assert validate_file_extension("book.pdf") is True
        assert validate_file_extension("IMAGE.JPG") is False # Default allow list is pdf, epub, mobi
        assert validate_file_extension("script.exe") is False
        assert validate_file_extension("code.py") is False

    # 3. Tests de File Signature (Magic Numbers)
    def test_validate_signature_pdf(self):
        # PDF empieza con %PDF
        content = b"%PDF-1.4\n..."
        assert validate_file_signature(content, 'pdf') is True
        
        # Fake PDF
        fake_content = b"<html><body>Not a PDF</body></html>"
        assert validate_file_signature(fake_content, 'pdf') is False

    def test_validate_signature_epub(self):
        # EPUB es un ZIP (PK..)
        content = b"PK\x03\x04\x14\x00..."
        assert validate_file_signature(content, 'epub') is True

    # 4. Content Length
    def test_validate_content_length(self):
        # 1MB OK
        headers = {'Content-Length': '1048576'}
        assert validate_content_length(headers, max_size_mb=10)[0] is True
        
        # 11MB Fail (limit 10)
        headers_big = {'Content-Length': '11534336'}
        assert validate_content_length(headers_big, max_size_mb=10)[0] is False

        # Missing header
        assert validate_content_length({}, max_size_mb=10)[0] is False

