
import logging
import sys
import os
import json
import mariadb
from database import DB_CONFIG, get_db_cursor
from services.enrichment.processor import BookEnrichmentProcessor
from services.enrichment.efficiency import EfficiencyManager
from services.enrichment.strategies import EnrichmentStrategy, get_books_to_process_query

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('test_enrichment')

def run_diagnostic():
    print("=== DIAGNÓSTICO DE ENRIQUECIMIENTO REFACORIZADO ===")
    
    conn = None
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # 1. Instanciar Componentes
        print("\n1. Instanciando componentes...")
        eff_mgr = EfficiencyManager(conn)
        processor = BookEnrichmentProcessor(conn, eff_mgr)
        print("   [OK] EfficiencyManager y BookEnrichmentProcessor creados.")
        
        # 2. Probar Estrategia de Selección
        print("\n2. Probando selección de libros (Estrategia: SMART)...")
        enterprise_id = 1
        limit = 5
        sql, params = get_books_to_process_query(EnrichmentStrategy.SMART, enterprise_id, limit, deep_scan=True)
        print(f"   [SQL]: {sql.strip()[:100]}...")
        cursor.execute(sql, params)
        books = cursor.fetchall()
        print(f"   [OK] Libros encontrados para procesar: {len(books)}")

        # 3. Probar Plan de Ejecución
        print("\n3. Probando plan de ejecución para un libro...")
        if books:
            test_book = books[0]
            ranking = eff_mgr.get_service_ranking()
            plan = processor.build_execution_plan(test_book, enterprise_id, ranking, deep_scan=True)
            print(f"   [PLAN]: {[s[0] for s in plan]}")
            if len(plan) > 0:
                print("   [OK] Plan generado correctamente.")
            else:
                print("   [!] Plan vacío. Verifique configuración de servicios.")
        else:
            print("   [SKIP] No hay libros para probar el plan.")

        # 4. Prueba de Enriquecimiento (Live Test con ISBN conocido si es posible)
        # Harry Potter and the Philosopher's Stone ISBN-13: 9781408855652
        print("\n4. Prueba de enriquecimiento LIVE (ISBN: 9781408855652)...")
        dummy_book = {
            'id': 99999, # Dummy ID
            'nombre': 'Test Harry Potter',
            'codigo': '9781408855652',
            'tipo_articulo_id': 1,
            'metadata_json': '{}'
        }
        
        # Mocking update_book_record to avoid writing to DB
        original_update = processor.update_book_record
        processor.update_book_record = lambda b, m, a: print(f"      (Simulación) Registro actualizado para {b['nombre']}")
        
        success, api_data = processor.enrich_book(dummy_book, enterprise_id, ranking, deep_scan=False)
        
        if success:
            print(f"   [OK] Libro enriquecido exitosamente.")
            print(f"   [DATA]: Título: {api_data.get('titulo')}, Editorial: {api_data.get('editorial')}")
            print(f"   [SERVICIOS]: {api_data.get('fuente', 'Varios')}")
        else:
            print("   [!] El enriquecimiento no obtuvo resultados (puede ser normal si las APIs están limitadas).")
        
        # Restaurar método original
        processor.update_book_record = original_update

        # 5. Verificar Eficiencia (Read-only)
        print("\n5. Consultando ranking de eficiencia actual...")
        ranking = eff_mgr.get_service_ranking()
        print(f"   [RANKING]: {ranking}")
        print("   [OK] Consulta de eficiencia completada.")

    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\nConexión cerrada.")

if __name__ == "__main__":
    run_diagnostic()
