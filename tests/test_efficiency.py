
import pytest
from unittest.mock import MagicMock
from services.enrichment.efficiency import EfficiencyManager

class TestEfficiencyManager:

    @pytest.fixture
    def mock_db(self):
        """Fixture que provee una conexión y cursor mockeados."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_get_service_ranking(self, mock_db):
        conn, cursor = mock_db
        # Simulamos respuesta de la BD
        cursor.fetchall.return_value = [('Google Books',), ('Librario',), ('Mercado Libre',)]
        
        mgr = EfficiencyManager(conn)
        ranking = mgr.get_service_ranking()
        
        assert len(ranking) == 3
        assert ranking[0] == 'Google Books'
        cursor.execute.assert_called_once()

    def test_update_score(self, mock_db):
        conn, cursor = mock_db
        mgr = EfficiencyManager(conn)
        
        mgr.update_score('Open Library', 5, 1)
        
        # Verificar que se llamó a la query de INSERT/UPDATE
        args, _ = cursor.execute.call_args
        query = args[0]
        params = args[1]
        
        assert "INSERT INTO service_efficiency" in query
        assert "ON DUPLICATE KEY UPDATE" in query
        assert params == ('Open Library', 5, 1, 5, 1)

    def test_rotate_learning_cycle_threshold_not_reached(self, mock_db):
        conn, cursor = mock_db
        # Mock de respuesta: procesados = 150 (umbral es 300)
        cursor.fetchone.return_value = (150,)
        
        mgr = EfficiencyManager(conn)
        mgr.rotate_learning_cycle()
        
        # Debe haber incrementado y verificado, pero NO truncado
        assert cursor.execute.call_count == 2
        # Verificar que NO se llamó a TRUNCATE
        for call in cursor.execute.call_args_list:
            assert "TRUNCATE" not in call[0][0]

    def test_rotate_learning_cycle_reset(self, mock_db):
        conn, cursor = mock_db
        # Mock de respuesta: procesados = 300 (umbral alcanzado)
        cursor.fetchone.return_value = (300,)
        
        mgr = EfficiencyManager(conn)
        mgr.rotate_learning_cycle()
        
        # Debe haber truncado
        calls = [call[0][0] for call in cursor.execute.call_args_list]
        assert any("TRUNCATE TABLE service_efficiency" in c for c in calls)
        assert any("processed_since_reset = 0" in c for c in calls)
