
from services.scraping_service import CuspideScraper
import json

def test_cuspide():
    scraper = CuspideScraper()
    # Un ISBN conocido de Cúspide (ej: Harry Potter o algo común)
    isbn = "9789878000107" # El principito o similar
    print(f"Probando con ISBN: {isbn}")
    
    success, data = scraper.get_info(isbn)
    
    if success:
        print("Éxito!")
        print(json.dumps(data, indent=2))
    else:
        print("Fallo:", data)

if __name__ == "__main__":
    test_cuspide()
