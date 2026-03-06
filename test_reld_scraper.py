
from services.scraping_service import ReldScraper
import json

def test_reld():
    scraper = ReldScraper()
    code = "900224040"
    print(f"Probando con código: {code}")
    
    success, data = scraper.get_info(code)
    
    if success:
        print("Éxito!")
        print(json.dumps(data, indent=2))
    else:
        print("Fallo:", data)

if __name__ == "__main__":
    test_reld()
