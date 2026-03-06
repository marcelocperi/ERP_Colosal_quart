import requests
import sys
import os

# Ajustar path para importar core si es necesario
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.security_utils import (
    sanitize_filename, 
    validate_file_extension, 
    validate_file_signature, 
    validate_content_length
)

# Singleton Session for Performance Intelligence (Reusing TCP connections)
session = requests.Session()
# Set default headers to look like a browser if needed, or identify as biblioteca-bot
session.headers.update({
    'User-Agent': 'BibliotecaWeb-Enrichment-Bot/1.0 (contact: admin@example.com)'
})

def get_book_info_by_isbn(isbn):
    """
    Consulta la API de Open Library para validar un ISBN y obtener información básica.
    Retorna (True, data) si es válido, (False, error_msg) si no lo es.
    """
    # Limpiar ISBN de guiones o espacios
    clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
    
    # Intentamos consultar la API de Open Library
    url = f"https://openlibrary.org/isbn/{clean_isbn}.json"
    
    try:
        response = session.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            languages = data.get("languages", [])
            lang_code = None
            if languages:
                lang_code = languages[0].get("key", "").split('/')[-1]
                
            covers = data.get("covers", [])
            cover_id = covers[0] if covers else None
            cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else None

            # Extraer descripción (puede venir como string o como objeto con 'value')
            description_raw = data.get("description", "")
            if isinstance(description_raw, dict):
                description = description_raw.get("value", "")
            else:
                description = description_raw
            
            # Extraer enlaces de ebook si existen
            ebooks = data.get("ebooks", [])
            ebook_url = None
            if ebooks:
                ebook_url = ebooks[0].get("preview_url") or ebooks[0].get("read_url")

            return True, {
                "titulo": data.get("title"),
                "paginas": data.get("number_of_pages"),
                "fecha_pub": data.get("publish_date"),
                "lengua": lang_code,
                "temas": data.get("subjects", []),
                "descripcion": description,
                "cover_url": cover_url,
                "editorial": data.get("publishers", [""])[0] if data.get("publishers") else None,
                "identifiers": {
                    "lccn": data.get("lccn", []),
                    "oclc": data.get("oclc", []),
                    "work_id": data.get("works", [{}])[0].get("key", "").split('/')[-1] if data.get("works") else None
                },
                "ebook_access": {
                    "url": ebook_url,
                    "source": "Open Library / Archive.org"
                } if ebook_url else None,
                "info_raw": data
            }
        elif response.status_code == 404:
            return False, "ISBN no encontrado en el registro mundial de Open Library."
        else:
            return False, f"Servicio de validación no disponible (Status: {response.status_code})."
            
    except requests.exceptions.RequestException as e:
        return False, f"Error de conexión con el validador de ISBN: {str(e)}"

def search_books_by_language(lang_code, limit=10):
    """
    Busca libros en Open Library por idioma.
    Retorna una lista de diccionarios con la info de los libros encontrados.
    """
    url = "https://openlibrary.org/search.json"
    params = {
        'q': f'language:{lang_code}',
        'limit': limit,
        'fields': 'title,author_name,isbn,first_publish_year,publisher,number_of_pages_median',
        'sort': 'random'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        books = []
        for doc in data.get('docs', []):
            # Prefer ISBN-13, then ISBN-10
            isbns = doc.get('isbn', [])
            if not isbns:
                continue # Skip books without ISBN as we need it for PK
            
            # Simple heuristic: take first ISBN-13, else first ISBN
            clean_isbn = next((i for i in isbns if len(i) == 13), isbns[0])
            
            book = {
                'title': doc.get('title', 'Unknown Title'),
                'author': doc.get('author_name', ['Unknown'])[0],
                'isbn': clean_isbn,
                'year': doc.get('first_publish_year', 0),
                'publisher': doc.get('publisher', ['Unknown'])[0],
                'pages': doc.get('number_of_pages_median', 0)
            }
            books.append(book)
            
        return books

    except Exception as e:
        print(f"Error fetching books by language: {e}")
        return []

def search_books_by_query(query, limit=10):
    """
    Busca libros en Open Library por término general (título, autor, ISBN, etc).
    Retorna una lista de diccionarios con la info de los libros encontrados.
    """
    url = "https://openlibrary.org/search.json"
    params = {
        'q': query,
        'limit': limit,
        'fields': 'title,author_name,isbn,first_publish_year,publisher,number_of_pages_median,cover_i',
        'sort': 'editions'  # Priorizar libros con más ediciones (populares)
    }
    
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        books = []
        for doc in data.get('docs', []):
            # Prefer ISBN-13, then ISBN-10
            isbns = doc.get('isbn', [])
            if not isbns:
                continue 
            
            clean_isbn = next((i for i in isbns if len(i) == 13), isbns[0])
            
            # Intentar obtener la mejor imagen de portada disponible
            cover_id = doc.get('cover_i')
            cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg" if cover_id else None

            book = {
                'title': doc.get('title', 'Unknown Title'),
                'author': doc.get('author_name', ['Unknown'])[0],
                'isbn': clean_isbn,
                'year': doc.get('first_publish_year', 0),
                'publisher': doc.get('publisher', ['Unknown'])[0],
                'pages': doc.get('number_of_pages_median', 0),
                'cover_url': cover_url
            }
            books.append(book)
            
        return books
    except Exception as e:
        print(f"Error fetching books by query: {e}")
        return []

def search_books_by_language(lang_code, limit=10, source='openlibrary'):
    """
    Busca libros por idioma usando diferentes fuentes.
    Fuentes soportadas: 'openlibrary', 'google', 'worldcat' (via Google/Fallback)
    """
    if source == 'google':
        return search_books_from_google(f'language:{lang_code}', limit)
    elif source == 'worldcat':
        # WorldCat search via Google is often better for a broad 'MundoCat' experience
        return search_books_from_google(f'language:{lang_code}', limit)
    
    # Default: Open Library
    return search_books_from_openlibrary(lang_code, limit)

def search_books_from_openlibrary(lang_code, limit=10):
    url = "https://openlibrary.org/search.json"
    params = {
        'q': f'language:{lang_code}',
        'limit': limit,
        'fields': 'title,author_name,isbn,first_publish_year,publisher,number_of_pages_median',
        'sort': 'random'
    }
    
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        books = []
        for doc in data.get('docs', []):
            isbns = doc.get('isbn', [])
            if not isbns: continue
            clean_isbn = next((i for i in isbns if len(i) == 13), isbns[0])
            
            books.append({
                'title': doc.get('title', 'Unknown Title'),
                'author': doc.get('author_name', ['Unknown'])[0],
                'isbn': clean_isbn,
                'year': doc.get('first_publish_year', 0),
                'publisher': doc.get('publisher', ['Unknown'])[0],
                'pages': doc.get('number_of_pages_median', 0)
            })
        return books
    except Exception as e:
        print(f"Error Open Library: {e}")
        return []

def search_books_from_google(query, limit=10):
    """
    Busca libros en Google Books por término.
    """
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        'q': query,
        'maxResults': min(limit, 40),
        'printType': 'books'
    }
    
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        books = []
        for item in data.get('items', []):
            info = item.get('volumeInfo', {})
            # Get ISBN
            isbns = info.get('industryIdentifiers', [])
            isbn = None
            for id_obj in isbns:
                if id_obj.get('type') == 'ISBN_13':
                    isbn = id_obj.get('identifier')
                    break
            if not isbn and isbns:
                isbn = isbns[0].get('identifier')
            
            if not isbn: continue
            
            books.append({
                'title': info.get('title', 'Sin título'),
                'author': ", ".join(info.get('authors', [])) if info.get('authors') else 'Desconocido',
                'isbn': isbn,
                'year': info.get('publishedDate', '0')[:4] if info.get('publishedDate') else 0,
                'publisher': info.get('publisher', 'Desconocido'),
                'pages': info.get('pageCount', 0)
            })
        return books
    except Exception as e:
        print(f"Error Google Search: {e}")
        return []

def get_book_info_from_worldcat(isbn):
    """
    Consulta WorldCat (MundoCat) para obtener información.
    Como el API de búsqueda es restrictivo, usamos el agregador Librario o Classify como fallback.
    """
    # Intentamos primero vía Librario que ya integra WorldCat
    success, data = get_book_info_from_librario(isbn)
    if success:
        data['fuente'] = "WorldCat (MundoCat)"
        return True, data
        
    # Fallback a un scrape básico o a Classify
    clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
    url = f"http://classify.oclc.org/classify2/Classify?isbn={clean_isbn}&summary=true"
    try:
        # requests maneja el 301 automáticamente
        response = session.get(url, timeout=10)
        if response.status_code == 200 and 'work' in response.text:
            # Es XML, pero podemos hacer un mini-parseo o usarlo para saber que existe
            # Por ahora, si no tenemos una librería XML pesada, confiamos en Librario
            return False, "MundoCat: Requiere parseo XML avanzado o Key de API."
    except: pass
    
    return False, "No se pudo obtener info de MundoCat directamente."
def get_book_info_from_google_books(isbn):
    """
    Consulta la API de Google Books para obtener información enriquecida.
    """
    clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{clean_isbn}"
    
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("totalItems", 0) > 0:
                item = data["items"][0]
                info = item.get("volumeInfo", {})
                access = item.get("accessInfo", {})
                
                # Imagen de mejor calidad si existe
                images = info.get("imageLinks", {})
                cover_url = images.get("thumbnail") or images.get("smallThumbnail")
                if cover_url and cover_url.startswith("http:"):
                    cover_url = cover_url.replace("http:", "https:")
                
                # Extraer enlaces de lectura/preview y DESCARGA real
                preview_link = info.get("previewLink")
                
                # Buscar links de descarga directa (EPUB/PDF)
                pdf_info = access.get("pdf", {})
                epub_info = access.get("epub", {})
                
                download_url = None
                is_direct = False
                
                # Priorizar links de descarga si están disponibles
                if pdf_info.get("isAvailable") and pdf_info.get("downloadLink"):
                    download_url = pdf_info.get("downloadLink")
                    is_direct = True
                elif epub_info.get("isAvailable") and epub_info.get("downloadLink"):
                    download_url = epub_info.get("downloadLink")
                    is_direct = True
                elif pdf_info.get("acsTokenLink"):
                    download_url = pdf_info.get("acsTokenLink")
                    is_direct = True
                
                ebook_url = download_url or preview_link

                return True, {
                    "titulo": info.get("title"),
                    "paginas": info.get("pageCount"),
                    "fecha_pub": info.get("publishedDate"),
                    "lengua": info.get("language"),
                    "temas": info.get("categories", []),
                    "descripcion": info.get("description", ""),
                    "cover_url": cover_url,
                    "autor": ", ".join(info.get("authors", [])) if info.get("authors") else None,
                    "editorial": info.get("publisher"),
                    "fuente": "Google Books",
                    "ebook_access": {
                        "url": ebook_url,
                        "source": "Google Books (Direct)" if is_direct else "Google Books Preview",
                        "is_direct": is_direct
                    } if ebook_url else None
                }
        return False, "No se encontró información en Google Books."
    except Exception as e:
        return False, f"Error Google Books: {str(e)}"

def get_book_info_from_librario(isbn):
    """
    Consulta la API de Librario.dev (Aggregator) para obtener información unificada.
    """
    clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
    url = f"https://api.librario.dev/v1/book/{clean_isbn}"
    
    # Example Token (Publicly mentioned in docs/HN)
    headers = {
        'Authorization': 'Bearer librario_ARbmrp1fjBpDywzhvrQcByA4sZ9pn7D5HEk0kmS34eqRcaujyt0enCZ',
        'Accept': 'application/json'
    }
    
    try:
        response = session.get(url, headers=headers, timeout=12)
        if response.status_code == 200:
            data = response.json()
            
            # Map Librario schema to our internal schema
            authors = [a.get('name') for a in data.get('authors', []) if a.get('name')]
            categories = [c.get('name') for c in data.get('categories', []) if c.get('name')]
            
            return True, {
                "titulo": data.get("title"),
                "paginas": data.get("pageCount"),
                "fecha_pub": data.get("publicationDate"),
                "lengua": data.get("language"),
                "temas": categories,
                "descripcion": data.get("synopsis") or data.get("description", ""),
                "cover_url": data.get("coverUrl"),
                "autor": ", ".join(authors) if authors else None,
                "editorial": data.get("publisher"),
                "fuente": "Librario"
            }
        elif response.status_code == 500:
             return False, "Librario API: Error interno del servidor (posible pre-alpha instability)"
        return False, f"Librario API: No se encontró info o error {response.status_code}"
    except Exception as e:
        return False, f"Error Librario: {str(e)}"
    
def is_downloadable(url):
    """
    Verifica si una URL apunta a un archivo descargable (PDF, EPUB) verificando el Content-Type.
    """
    if not url: return False
    h = session.head(url, allow_redirects=True, timeout=5)
    header = h.headers
    content_type = header.get('content-type', '').lower()
    
    # Lista de tipos MIME permitidos para descarga
    allowed_types = [
        'application/pdf',
        'application/epub+zip',
        'application/x-mobipocket-ebook',
        'application/octet-stream' # Genérico, a veces usado para binarios
    ]
    
    if any(t in content_type for t in allowed_types):
        return True
    return False

def download_ebook_content(url):
    """
    Descarga el contenido binario de un ebook si es accesible.
    Retorna (contenido_bytes, tipo_mime, nombre_sugerido)
    """
    try:
        # 1. Validar Content-Length antes de descargar
        r_head = session.head(url, allow_redirects=True, timeout=5)
        is_valid_size, size = validate_content_length(r_head.headers)
        
        if not is_valid_size:
            print(f"⚠️ Archivo demasiado grande o sin Content-Length: {url}")
            return None, None, None

        # 2. Descargar con timeout
        r = session.get(url, allow_redirects=True, timeout=30)
        
        if r.status_code == 200:
            content_type = r.headers.get('content-type', '').lower()
            
            # Validación estricta: Rechazar HTML, JSON, XML (salvo EPUB)
            if 'html' in content_type or 'json' in content_type or ('xml' in content_type and 'epub' not in content_type):
                return None, content_type, None
            
            # Verificar si es un tipo de ebook válido o binario
            valid_types = ['pdf', 'epub', 'mobi', 'octet-stream', 'text/plain']
            if not any(t in content_type for t in valid_types):
                 return None, content_type, None

            # Limpiar content type para devolver solo la parte MIME
            simple_mime = content_type.split(';')[0].strip()
            
            # Intentar obtener nombre del archivo del header Content-Disposition
            filename = "ebook_descargado"
            if "Content-Disposition" in r.headers:
                import re
                fname = re.findall("filename=(.+)", r.headers["Content-Disposition"])
                if fname: 
                    # 3. Sanitizar filename
                    filename = sanitize_filename(fname[0].strip('"'))
            
            # Extension basada en content-type si filename es generico
            if filename == "ebook_descargado":
                if "pdf" in simple_mime: filename += ".pdf"
                elif "epub" in simple_mime: filename += ".epub"
                elif "mobi" in simple_mime: filename += ".mobi"
                elif "text/plain" in simple_mime: filename += ".txt"
            
            # 4. Validar extensión permitida
            if not validate_file_extension(filename):
                print(f"⚠️ Extensión no permitida: {filename}")
                return None, content_type, None
            
            # 5. Validar signature (magic numbers) para PDF/EPUB
            ext_check = filename.split('.')[-1].lower()
            if ext_check in ['pdf', 'epub']:
                if not validate_file_signature(r.content, ext_check):
                   print(f"⚠️ File signature mismatch: {filename} ({ext_check})")
                   return None, content_type, None

            return r.content, content_type, filename
    except Exception as e:
        print(f"Error descargando ebook: {e}")
    return None, None, None
