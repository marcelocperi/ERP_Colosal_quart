
import requests
from bs4 import BeautifulSoup
import re

def test_amazon(isbn):
    url = f"https://www.amazon.com/s?k={isbn}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    }
    print(f"Consultando Amazon para ISBN: {isbn}")
    try:
        # Usamos requests directo para debuguear sin TOR primero
        response = requests.get(url, headers=headers, timeout=15)
        print(f"Status: {response.status_code}")
        
        if "api-services-support@amazon.com" in response.text or "To discuss automated access to Amazon data please contact" in response.text:
            print("CAPCHA DETECTADO / BLOQUEO")
            return
            
        soup = BeautifulSoup(response.text, 'html.parser')
        result = soup.select_one('.s-result-item[data-component-type="s-search-result"]')
        
        if result:
            title_elem = result.select_one('h2 span')
            title = title_elem.text.strip() if title_elem else "N/A"
            img_elem = result.select_one('.s-image')
            cover_url = img_elem['src'] if img_elem else "N/A"
            print(f"Resultado Encontrado:")
            print(f"Título: {title}")
            print(f"Cover: {cover_url}")
        else:
            print("No se encontró el selector de resultados.")
            # Print a bit of the body to see what we got
            print("Snippet del body:")
            print(response.text[:500])
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_amazon("9780307474728") # Cien años de soledad
