import os
import re
from pathlib import Path

def find_modals_without_cancel():
    """Encuentra todos los modales que no tienen botón de cancelar"""
    
    templates_dir = Path('.')
    modals_info = []
    
    # Buscar todos los archivos HTML
    for html_file in templates_dir.rglob('*.html'):
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Buscar modales (divs con id que contengan 'modal')
            modal_pattern = r'<div[^>]*id=["\']([^"\']*modal[^"\']*)["\'][^>]*>(.*?)</div>\s*(?=<div|<script|</body|{% endblock %}|$)'
            
            modals = re.finditer(modal_pattern, content, re.IGNORECASE | re.DOTALL)
            
            for match in modals:
                modal_id = match.group(1)
                modal_content = match.group(2)
                
                # Verificar si tiene modal-footer
                has_footer = 'modal-footer' in modal_content
                
                # Buscar botones de cancelar
                cancel_patterns = [
                    r'toggleModal\(["\']' + re.escape(modal_id),
                    r'btn.*cancelar',
                    r'type=["\']button["\'].*close',
                    r'btn-close',
                    r'data-dismiss=["\']modal["\']'
                ]
                
                has_cancel = any(re.search(pattern, modal_content, re.IGNORECASE) for pattern in cancel_patterns)
                
                # Buscar botón de submit
                has_submit = bool(re.search(r'type=["\']submit["\']', modal_content, re.IGNORECASE))
                
                # Buscar título del modal
                title_match = re.search(r'<h[1-6][^>]*class=["\'][^"\']*modal-title[^"\']*["\'][^>]*>(.*?)</h[1-6]>', modal_content, re.IGNORECASE | re.DOTALL)
                title = title_match.group(1) if title_match else 'Sin título'
                title = re.sub(r'<[^>]+>', '', title).strip()  # Limpiar HTML
                
                modals_info.append({
                    'file': str(html_file),
                    'modal_id': modal_id,
                    'title': title,
                    'has_footer': has_footer,
                    'has_cancel': has_cancel,
                    'has_submit': has_submit
                })
        
        except Exception as e:
            print(f"Error procesando {html_file}: {e}")
    
    # Mostrar resultados
    print("=== ANÁLISIS DE MODALES ===\n")
    
    modals_without_cancel = [m for m in modals_info if not m['has_cancel'] and m['has_submit']]
    
    if modals_without_cancel:
        print(f"❌ MODALES SIN BOTÓN CANCELAR ({len(modals_without_cancel)}):\n")
        for modal in modals_without_cancel:
            print(f"📄 {modal['file']}")
            print(f"   ID: {modal['modal_id']}")
            print(f"   Título: {modal['title']}")
            print(f"   Footer: {'✅' if modal['has_footer'] else '❌'}")
            print(f"   Submit: {'✅' if modal['has_submit'] else '❌'}")
            print()
    else:
        print("✅ Todos los modales tienen botón de cancelar\n")
    
    # Resumen
    print(f"\n=== RESUMEN ===")
    print(f"Total de modales encontrados: {len(modals_info)}")
    print(f"Modales sin cancelar: {len(modals_without_cancel)}")
    print(f"Modales con cancelar: {len(modals_info) - len(modals_without_cancel)}")
    
    # Guardar reporte
    with open('modal_analysis.txt', 'w', encoding='utf-8') as f:
        f.write("=== MODALES SIN BOTÓN CANCELAR ===\n\n")
        for modal in modals_without_cancel:
            f.write(f"Archivo: {modal['file']}\n")
            f.write(f"ID: {modal['modal_id']}\n")
            f.write(f"Título: {modal['title']}\n")
            f.write(f"Footer: {'Sí' if modal['has_footer'] else 'No'}\n")
            f.write(f"Submit: {'Sí' if modal['has_submit'] else 'No'}\n")
            f.write("\n" + "="*50 + "\n\n")
    
    print(f"\n✅ Reporte guardado en modal_analysis.txt")
    
    return modals_without_cancel

if __name__ == "__main__":
    find_modals_without_cancel()
