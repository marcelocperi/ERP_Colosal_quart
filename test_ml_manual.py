
from services.scraping_service import MercadoLibreScraper
import logging

logging.basicConfig(level=logging.INFO)

def test_ml():
    scraper = MercadoLibreScraper()
    success, data = scraper.get_info("9789876703554")
    print(f"Success: {success}")
    if success:
        print(f"Título: {data.get('titulo')}")
        print(f"Cover: {data.get('cover_url')}")
    else:
        print(f"Error: {data}")

if __name__ == "__main__":
    test_ml()
