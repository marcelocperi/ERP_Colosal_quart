
import logging
import sys
import os
from datetime import datetime

# Setup logging to console to see what happens
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from enrich_books_api import process_books_batch
import mariadb
from database import DB_CONFIG

def test_waterfall():
    ent_id = 1
    target_id = 3 # UML
    
    conn = mariadb.connect(**DB_CONFIG)
    cur = conn.cursor()
    # Forzar a que no esté checked para que el batch lo tome (reset api_checked to 0)
    cur.execute("UPDATE stk_articulos SET api_checked = 0 WHERE id = %s", (target_id,))
    conn.commit()
    conn.close()
    
    print(f"--- Iniciando enriquecimiento forzado para ID {target_id} ---")
    # Solo procesamos 1 para estar seguros
    process_books_batch(limit=1, deep_scan=True, enterprise_id=ent_id)
    
    # Verificar resultado
    conn = mariadb.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT nombre, metadata_json FROM stk_articulos WHERE id = %s", (target_id,))
    r = cur.fetchone()
    import json
    meta = json.loads(r['metadata_json']) if r['metadata_json'] else {}
    
    print(f"\nRESULTADO FINAL:")
    print(f"Título: {r['nombre']}")
    print(f"Portada: {meta.get('cover_url')}")
    print(f"Fuente final: {meta.get('fuente', 'Primaria')}")
    print(f"Descripción: {meta.get('descripcion')[:150] if meta.get('descripcion') else 'Ninguna'}...")
    conn.close()

if __name__ == "__main__":
    test_waterfall()
