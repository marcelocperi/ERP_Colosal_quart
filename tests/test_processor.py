
import pytest
from unittest.mock import MagicMock, patch
from services.enrichment.processor import BookEnrichmentProcessor

class TestBookEnrichmentProcessor:

    @pytest.fixture
    def mock_deps(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        efficiency_mgr = MagicMock()
        return conn, cursor, efficiency_mgr

    def test_get_existing_files_map(self, mock_deps):
        conn, cursor, eff = mock_deps
        cursor.fetchall.return_value = [{'articulo_id': 1}, {'articulo_id': 3}]
        
        processor = BookEnrichmentProcessor(conn, eff)
        res = processor.get_existing_files_map([1, 2, 3])
        
        assert res == {1, 3}
        assert 2 not in res
        cursor.execute.assert_called_once()
        assert "WHERE articulo_id IN (%s,%s,%s)" in cursor.execute.call_args[0][0]

    def test_build_execution_plan_deep(self, mock_deps):
        conn, cursor, eff = mock_deps
        processor = BookEnrichmentProcessor(conn, eff)
        
        lib = {'tipo_articulo_id': 1}
        plan = processor.build_execution_plan(lib, 1, ['Librario'], deep_scan=True)
        
        # Debe tener muchos niveles en deep scan
        assert len(plan) > 5
        # Google debe estar (por secuencia de usuario)
        assert any(p[0] == 'Google Books' for p in plan)

    @patch('services.enrichment.processor.__import__')
    def test_enrich_book_stop_early(self, mock_import, mock_deps):
        conn, cursor, eff = mock_deps
        processor = BookEnrichmentProcessor(conn, eff)
        
        # Simular que el primer servicio ya devuelve todo completo
        mock_service = MagicMock()
        mock_service.get_info.return_value = (True, {
            'cover_url': 'http://ok',
            'descripcion': 'ok',
            'temas': ['ok'],
            'paginas': 100,
            'editorial': 'ok'
        })
        
        # Inyectar el mock en el dispatch
        with patch.object(processor, 'service_map', {'Test': ('Test', 'mod', 'target')}):
             # Mock __import__ para devolver nuestro target mock
             mock_mod = MagicMock()
             mock_import.return_value = mock_mod
             setattr(mock_mod, 'target', lambda x: (True, {
                'cover_url': 'http://ok', 'descripcion': 'ok', 'temas': ['ok'], 'paginas': 100, 'editorial': 'ok'
             }))
             
             lib = {'id': 1, 'nombre': 'Test'}
             success, data = processor.enrich_book(lib, 1, [], deep_scan=False, has_file=True)
             
             assert success
             assert data['cover_url'] == 'http://ok'
