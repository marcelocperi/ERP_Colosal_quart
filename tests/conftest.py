
import pytest
from unittest.mock import MagicMock
import sys
import os

# Ajustar path para que los tests encuentren el código
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture
def mock_db_cursor(mocker):
    """
    Simula una conexión a Base de Datos y devuelve el cursor mockeado.
    Útil para verificar qué SQL se ejecuta.
    """
    # Mockear mariadb.connect
    mock_mariadb = mocker.patch('mariadb.connect')
    
    # Mockear conexión y cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Configurar que connect() devuelva nuestra conexión falsa
    mock_mariadb.return_value = mock_conn
    # Configurar que conn.cursor() devuelva nuestro cursor falso
    mock_conn.cursor.return_value = mock_cursor
    
    return mock_cursor
