import re
from bs4 import BeautifulSoup
from services.rotation_service import rotation_manager as tor_manager

class CuspideScraper:
    """
    Scraper para obtener información de libros desde Cúspide (Argentina).
    Útil para libros en español que no están en Open Library.
    """
    BASE_URL = "https://www.cuspide.com"
    SEARCH_URL = "https://www.cuspide.com/resultados.aspx?c={isbn}&por=isbn"

    def get_info(self, isbn):
        try:
            # Limpieza básica del ISBN
            clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
            
            # 1. Buscar el libro
            url = self.SEARCH_URL.format(isbn=clean_isbn)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = tor_manager.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                return False, f"Error al conectar con Cúspide: {response.status_code}"
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Buscar si hay resultados
            # En Cúspide, si hay un solo resultado, a veces redirige al detalle, o muestra lista
            # Buscamos el contenedor de artículos
            articles = soup.select('article.producto')
            
            if not articles:
                # Intentar ver si redirigió a la página del producto directo
                if "libro/" in response.url:
                    product_page = response
                else:
                    return False, "Libro no encontrado en Cúspide"
            else:
                # Tomar el primer resultado y navegar a su detalle
                first_match = articles[0]
                link = first_match.select_one('a')['href']
                if not link.startswith('http'):
                    link = self.BASE_URL + link
                
                # Request al detalle
                product_page = tor_manager.get(link, headers=headers, timeout=10)
                if product_page.status_code != 200:
                    return False, "Error al obtener detalle del libro"

            # 2. Parsear el detalle del libro
            soup_det = BeautifulSoup(product_page.text, 'html.parser')
            
            # Título
            title = soup_det.select_one('h1.page-title')
            title = title.text.strip() if title else "Desconocido"
            
            # Autor
            author_div = soup_det.select_one('.autor a') # A veces es un div o a
            author = author_div.text.strip() if author_div else "Desconocido"
            
            # Imagen/Portada
            img = soup_det.select_one('#img_portada')
            cover_url = img['src'] if img else None
            if cover_url and not cover_url.startswith('http'):
                cover_url = self.BASE_URL + cover_url
            
            # Ficha técnica (paginas, año, etc)
            # Buscamos en los li de ficha técnica
            # Estructura usual: <li><span>Páginas:</span> 400</li>
            meta_dict = {}
            for li in soup_det.select('.datos-ficha li'):
                text = li.text.strip()
                if ':' in text:
                    key, val = text.split(':', 1)
                    meta_dict[key.strip().lower()] = val.strip()

            pages = 0
            year = 0
            if 'páginas' in meta_dict:
                try: pages = int(re.sub(r'\D', '', meta_dict['páginas']))
                except: pass
            
            if 'publicación' in meta_dict:
                # A veces es fecha completa o solo año
                year_match = re.search(r'\d{4}', meta_dict['publicación'])
                if year_match: year = int(year_match.group(0))

            editorial = "Desconocido"
            if 'editorial' in meta_dict:
                editorial = meta_dict['editorial']

            # Sinopsis
            # Suele estar en un div con id 'sinopsis' o clase
            desc_div = soup_det.select_one('#sinopsis')
            descripcion = desc_div.text.strip() if desc_div else ""
            
            # Género/Temas (Breadcrumbs)
            temas = []
            crumbs = soup_det.select('.breadcrumb li a')
            for c in crumbs:
                t = c.text.strip()
                if t and t not in ['Inicio', 'Libros']:
                    temas.append(t)

            return True, {
                "titulo": title,
                "author": author,
                "paginas": pages,
                "fecha_pub": str(year) if year else None,
                "lengua": "spa", # Asumimos español en Cúspide mayormente
                "temas": temas,
                "descripcion": descripcion,
                "cover_url": cover_url,
                "editorial": editorial,
                "identifiers": {},
                "info_raw": meta_dict 
            }

        except Exception as e:
            return False, f"Error en scraping Cúspide: {str(e)}"

class ReldScraper:
    """
    Scraper para Reld (Repuestos y Refrigeración).
    Mapea datos directamente al modelo de artículos ERP.
    """
    BASE_URL = "https://www.reld.com.ar"
    # El sitio usa un parámetro cod_articulo para acceso directo
    SEARCH_URL = "https://www.reld.com.ar/articulo.php?cod_articulo={code}"
    # Y un parámetro articulo en index_grilla.php para búsqueda general
    GRID_URL = "https://www.reld.com.ar/index_grilla.php?articulo={code}"

    def get_info(self, code):
        try:
            # 1. Intentar acceso directo por código
            url = self.SEARCH_URL.format(code=code)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = tor_manager.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                return False, f"Error al conectar con Reld: {response.status_code}"
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Verificar si el código existe en la página
            product_title = soup.select_one('.destacado-titulo')
            
            if not product_title:
                # Intentar búsqueda en grilla si el acceso directo falló
                url_grid = self.GRID_URL.format(code=code)
                response = tor_manager.get(url_grid, headers=headers, timeout=10)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # En la grilla, buscamos el primer link de producto
                # Selector para los artículos en la grilla (basado en inspección previa)
                items = soup.select('.item-articulo a') 
                if not items:
                    return False, f"Producto {code} no encontrado en Reld"
                
                # Ir al primero
                link = items[0]['href']
                if not link.startswith('http'):
                    link = self.BASE_URL + "/" + link
                
                response = tor_manager.get(link, headers=headers, timeout=10)
                soup = BeautifulSoup(response.text, 'html.parser')
                product_title = soup.select_one('.destacado-titulo')

            if not product_title:
                return False, "No se pudo identificar la página del producto"

            title = product_title.text.strip()
            
            # Precio
            price_elem = soup.select_one('#valor_precio')
            precio_str = price_elem.text.strip() if price_elem else ""
            
            # Limpiar precio para obtener solo el número
            precio_num = 0
            if precio_str:
                try:
                    # Formato usual: $ 1.234,56
                    clean_price = re.sub(r'[^\d,]', '', precio_str).replace(',', '.')
                    precio_num = float(clean_price)
                except: pass

            # Brand/Marca
            # Reld suele poner la marca en la descripción o en el título
            brand = "Genérico"
            title_parts = title.split(' ')
            if title_parts:
                brand = title_parts[-1] # A veces la marca está al final en Reld

            # SKU/Code de verificación
            sku_elem = soup.select_one('#codi_arti')
            real_sku = sku_elem.text.strip() if sku_elem else code

            # Descripción / Ficha Técnica
            desc_div = soup.select_one('.articulo-info-adic')
            short_desc = desc_div.text.strip() if desc_div else ""
            if short_desc.startswith("Descripción Adicional"):
                short_desc = short_desc.replace("Descripción Adicional", "").strip()
            
            # Image
            # La imagen tiene la clase 'agrandar' directamente
            img_elem = soup.select_one('img.agrandar') or soup.select_one('.swiper-slide-active img')
            img_url = img_elem['src'] if img_elem else None
            if img_url and not img_url.startswith('http'):
                img_url = self.BASE_URL + "/" + img_url

            # Category (Breadcrumbs)
            # El selector .breadcrumb b suele ser común en estos sitios
            categories = []
            cat_elems = soup.select('.breadcrumb a, .breadcrumb b')
            for c in cat_elems:
                t = c.text.strip()
                if t and t not in ['Inicio', 'Productos', '>']:
                    categories.append(t)


            return True, {
                "titulo": title,
                "marca": brand,
                "codigo": real_sku,
                "precio": precio_num,
                "precio_str": precio_str,
                "descripcion": short_desc,
                "tech_data": {},
                "temas": categories,
                "cover_url": img_url,
                "tipo_articulo_id": 2, # Repuestos
                "info_raw": {
                    "site": "Reld",
                    "url": response.url
                }
            }

        except Exception as e:
            return False, f"Error en scraping Reld: {str(e)}"

class AmazonScraper:
    """
    Scraper de respaldo para Amazon. Se usa principalmente para portadas y paginado.
    """
    SEARCH_URL = "https://www.amazon.com/s?k={isbn}"
    
    def get_info(self, isbn):
        try:
            clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
            url = self.SEARCH_URL.format(isbn=clean_isbn)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
            }
            
            response = tor_manager.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                return False, f"Amazon bloqueó el acceso o no respondió (Status {response.status_code})"
            
            soup = BeautifulSoup(response.text, 'html.parser')
            # Buscamos el primer resultado que parezca un libro
            result = soup.select_one('.s-result-item[data-component-type="s-search-result"]')
            if not result:
                return False, "Amazon: No se encontraron resultados"

            title_elem = result.select_one('h2 span')
            title = title_elem.text.strip() if title_elem else "Desconocido"
            
            img_elem = result.select_one('.s-image')
            cover_url = img_elem['src'] if img_elem else None
            
            # Intentar obtener info extra (páginas) si está en el texto
            text_total = result.get_text()
            pages = 0
            page_match = re.search(r'(\d+)\s+páginas', text_total, re.I)
            if page_match:
                pages = int(page_match.group(1))

            return True, {
                "titulo": title,
                "cover_url": cover_url,
                "paginas": pages,
                "fuente": "Amazon"
            }
        except Exception as e:
            return False, f"Error Amazon: {str(e)}"

class MercadoLibreScraper:
    """
    Scraper de respaldo para Mercado Libre Argentina. Muy útil para libros locales.
    """
    SEARCH_URL = "https://listado.mercadolibre.com.ar/{isbn}"
    
    def get_info(self, isbn):
        try:
            clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
            url = self.SEARCH_URL.format(isbn=clean_isbn)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
            }
            
            response = tor_manager.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                return False, f"ML bloqueó el acceso (Status {response.status_code})"
            
            soup = BeautifulSoup(response.text, 'html.parser')
            result = soup.select_one('.ui-search-result__wrapper')
            if not result:
                return False, "Mercado Libre: No se encontraron publicaciones"

            title_elem = result.select_one('.ui-search-item__title')
            title = title_elem.text.strip() if title_elem else "Desconocido"
            
            img_elem = result.select_one('.ui-search-result-image__element')
            cover_url = img_elem.get('data-src') or img_elem.get('src') if img_elem else None
            
            return True, {
                "titulo": title,
                "cover_url": cover_url,
                "fuente": "Mercado Libre"
            }
        except Exception as e:
            return False, f"Error ML: {str(e)}"

