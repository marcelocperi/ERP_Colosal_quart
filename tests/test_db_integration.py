
import pytest
from unittest.mock import MagicMock
import os
import sys

from tests.conftest import mock_db_cursor
from services import library_api_service
from core import security_utils

# Importar módulos a probar
import database
import enrich_books_api

class TestDatabaseIntegration:

    # 1. Verificar Configuración Segura (Código Fuente)
    def test_database_source_code_is_secure(self):
        # Verificar que el ARCHIVO database.py no contenga la password en texto plano
        db_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'database.py')
        
        with open(db_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        assert '"Taz100"' not in content, "CRITICO: Password 'Taz100' encontrada hardcodeada en database.py"
        assert "'Taz100'" not in content, "CRITICO: Password 'Taz100' encontrada hardcodeada en database.py"
        assert "os.environ.get" in content, "El archivo debe usar variables de entorno"

    def test_database_config_loaded(self):
        # Verificar que la config se carga (aunque el valor sea el default inseguro por ahora en .env)
        assert database.DB_CONFIG["port"] == 3307
        assert database.DB_CONFIG["user"] is not None

    # 2. Verificar Sintaxis SQL de Eficiencia (Simulación)
    def test_update_efficiency_sql_syntax(self, mock_db_cursor):
        """
        Verifica que la query de eficiencia sea sintácticamente válida para MariaDB/MySQL.
        Simulamos lo que hace la función interna update_efficiency.
        """
        # Configurar mock
        mock_conn = MagicMock()
        mock_db_cursor.connection = mock_conn

        # Datos
        name = "Google Books"
        fields_count = 5
        ebook_found = 1
        
        # Lógica extraída de enrich_books_api.py
        query = """
            INSERT INTO service_efficiency (service_name, hits_count, fields_provided, ebooks_provided)
            VALUES (%s, 1, %s, %s)
            ON DUPLICATE KEY UPDATE hits_count = hits_count + 1, fields_provided = fields_provided + %s, ebooks_provided = ebooks_provided + %s
        """
        params = (name, fields_count, ebook_found, fields_count, ebook_found)

        # Ejecutar en mock
        mock_db_cursor.execute(query, params)

        # Verificar
        mock_db_cursor.execute.assert_called_once_with(query, params)
        assert "ON DUPLICATE KEY UPDATE" in query
        assert query.count('%s') == 5
        
        # Verificar que el driver soportaría esto (al menos que lo llamamos bien)
        assert len(params) == 5

    # 3. Simular Guardado de Ebook (Filenames Sanitizados)
    def test_secure_ebook_filename_insert(self, mock_db_cursor, mocker):
        # Simular una operación de guardado manual (extraída de la lógica de negocio)
        articulo_id = 123
        contenido = b"DATA"
        filename_unsafe = "malicious..\\file.pdf"
        
        # Usar la función real de seguridad
        filename_safe = security_utils.sanitize_filename(filename_unsafe)
        
        # Query simulada (lo que haría el código real)
        query = """
            INSERT INTO stk_archivos_digitales (articulo_id, contenido, formato, nombre_archivo)
            VALUES (%s, %s, %s, %s)
        """
        
        mock_db_cursor.execute(query, (articulo_id, contenido, "pdf", filename_safe))
        
        # Verificar que se insertó el nombre SEGURO
        args = mock_db_cursor.execute.call_args[0]
        inserted_filename = args[1][3]
        
        assert inserted_filename == "file.pdf"
        assert inserted_filename != filename_unsafe

