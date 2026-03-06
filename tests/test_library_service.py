
import pytest
from unittest.mock import MagicMock
import sys
import os

# Ajustar path al root (multiMCP)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services import library_api_service

class TestLibraryApiService:

    # 1. Test Google Books API (Mockeado)
    def test_get_book_info_google_success(self, mocker):
        # Datos esperados
        isbn = "9781234567897"
        mock_response = {
            "totalItems": 1,
            "items": [{
                "volumeInfo": {
                    "title": "Clean Code",
                    "authors": ["Robert Martin"],
                    "description": "A handbook of agile software.",
                    "pageCount": 464,
                    "imageLinks": {"thumbnail": "http://img/cover.jpg"},
                    "publisher": "Prentice Hall",
                    "previewLink": "http://gb.com/read"
                },
                "accessInfo": {
                    "viewability": "ALL_PAGES",
                    "pdf": {"isAvailable": False},
                    "webReaderLink": "http://gb.com/read" 
                }
            }]
        }
        
        # Mockear requests.get dentro de session
        mock_get = mocker.patch.object(library_api_service.session, 'get')
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        # Ejecutar
        success, data = library_api_service.get_book_info_from_google_books(isbn)
        print(f"DEBUG SUCCESS: success={success}, data={data}")

        # Verificar
        assert success is True, f"Expected True, got {success} with data: {data}"
        assert data['titulo'] == "Clean Code"
        assert data['autor'] == "Robert Martin"
        assert data['paginas'] == 464
        assert data['ebook_access']['url'] == "http://gb.com/read"

    def test_get_book_info_google_not_found(self, mocker):
        isbn = "0000000000000"
        mock_response = {"totalItems": 0} # Google devuelve 0 items, no 404 siempre
        
        mock_get = mocker.patch.object(library_api_service.session, 'get')
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        # Ejecutar
        success, msg = library_api_service.get_book_info_from_google_books(isbn)
        print(f"DEBUG NOT FOUND: success={success}, msg={msg}")

        # Verificar
        assert success is False, f"Expected False, got True with message: {msg}"
        if msg:
             # Normalizar para evitar problemas de tildes/encoding
             msg_lower = msg.lower()
             assert "google books" in msg_lower or "informaci" in msg_lower, f"Unexpected message: {msg}"
        else:
             pytest.fail("Message is None")

    # 2. Test Download Ebook (Seguridad)
    def test_download_ebook_security_checks(self, mocker):
        # Caso: Content-Length faltante
        mock_head = mocker.patch.object(library_api_service.session, 'head')
        mock_head.return_value.headers = {} # Sin length
        
        content, mime, name = library_api_service.download_ebook_content("http://malicious.site/bigfile.zip")
        assert content is None # Debe rechazar

        # Caso: Extension prohibida (.exe)
        mock_head.return_value.headers = {'Content-Length': '100'}
        
        mock_get = mocker.patch.object(library_api_service.session, 'get')
        mock_get.return_value.status_code = 200
        mock_get.return_value.headers = {
            'content-type': 'application/x-msdownload', 
            'Content-Disposition': 'filename="virus.exe"'
        }
        
        content, mime, name = library_api_service.download_ebook_content("http://site.com/virus.exe")
        assert content is None # Debe rechazar por extensión

