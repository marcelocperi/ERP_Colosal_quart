import os
import re
from pathlib import Path

def find_modals_without_cancel_v2():
    """Encuentra todos los modales que no tienen botón de cancelar (versión mejorada)"""
    
    templates_dir = Path('.')
    modals_info = []
    
    # Buscar todos los archivos HTML
    for html_file in templates_dir.rglob('*.html'):
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Buscar modales más específicamente
            # Buscar divs con class que contenga 'modal' o id que contenga 'modal'
            modal_pattern = r'<div[^>]*(?:class=["\'][^"\']*modal[^"\']*["\']|id=["\']([^"\']*modal[^"\']*)["\'])[^>]*>(.*?)</div>\s*(?=(?:<div(?:\s|>)|<script|</body|{% endblock %}|$))'
            
            modals = re.finditer(modal_pattern, content, re.IGNORECASE | re.DOTALL)
            
            for match in modals:
                modal_id_match = re.search(r'id=["\']([^"\']*modal[^"\']*)["\']', match.group(0), re.IGNORECASE)
                modal_id = modal_id_match.group(1) if modal_id_match else 'sin-id'
                
                # Filtrar divs que claramente no son modales
                if any(keyword in modal_id.lower() for keyword in ['actions', 'content', 'body', 'header', 'footer', 'backdrop']):
                    continue
                
                modal_content = match.group(2) if match.lastindex >= 2 else match.group(0)
                
                # Verificar que tenga estructura de modal (form o modal-content)
                has_form = bool(re.search(r'<form', modal_content, re.IGNORECASE))
                has_modal_structure = bool(re.search(r'class=["\'][^"\']*modal-(content|dialog|body)[^"\']*["\']', modal_content, re.IGNORECASE))
                
                if not (has_form or has_modal_structure):
                    continue
                
                # Buscar botones de cancelar
                cancel_patterns = [
                    r'onclick=["\'][^"\']*toggleModal\(["\']' + re.escape(modal_id),
                    r'<button[^>]*>.*?cancelar.*?</button>',
                    r'btn-close',
                    r'data-dismiss=["\']modal["\']',
                    r'data-bs-dismiss=["\']modal["\']'
                ]
                
                has_cancel = any(re.search(pattern, modal_content, re.IGNORECASE) for pattern in cancel_patterns)
                
                # Buscar botón de submit
                has_submit = bool(re.search(r'type=["\']submit["\']', modal_content, re.IGNORECASE))
                
                # Buscar título del modal
                title_patterns = [
                    r'<h[1-6][^>]*(?:class=["\'][^"\']*modal-title[^"\']*["\']|id=["\'][^"\']*title[^"\']*["\'])[^>]*>(.*?)</h[1-6]>',
                    r'<h[1-6][^>]*>(.*?)</h[1-6]>'
                ]
                
                title = 'Sin título'
                for pattern in title_patterns:
                    title_match = re.search(pattern, modal_content, re.IGNORECASE | re.DOTALL)
                    if title_match:
                        title = title_match.group(1)
                        title = re.sub(r'<[^>]+>', '', title).strip()
                        if title:
                            break
                
                modals_info.append({
                    'file': str(html_file),
                    'modal_id': modal_id,
                    'title': title[:50],  # Limitar longitud
                    'has_cancel': has_cancel,
                    'has_submit': has_submit,
                    'has_form': has_form
                })
        
        except Exception as e:
            print(f"Error procesando {html_file}: {e}")
    
    # Filtrar modales que tienen submit pero no cancel
    modals_without_cancel = [m for m in modals_info if m['has_submit'] and not m['has_cancel']]
    
    # Mostrar resultados
    print("=== ANÁLISIS DE MODALES (V2 - Mejorado) ===\n")
    
    if modals_without_cancel:
        print(f"❌ MODALES SIN BOTÓN CANCELAR ({len(modals_without_cancel)}):\n")
        for modal in modals_without_cancel:
            print(f"📄 {modal['file']}")
            print(f"   ID: {modal['modal_id']}")
            print(f"   Título: {modal['title']}")
            print(f"   Form: {'✅' if modal['has_form'] else '❌'}")
            print(f"   Submit: {'✅' if modal['has_submit'] else '❌'}")
            print(f"   Cancel: {'✅' if modal['has_cancel'] else '❌'}")
            print()
    else:
        print("✅ Todos los modales con formularios tienen botón de cancelar\n")
    
    # Resumen
    print(f"\n=== RESUMEN ===")
    print(f"Total de modales encontrados: {len(modals_info)}")
    print(f"Modales con submit: {len([m for m in modals_info if m['has_submit']])}")
    print(f"Modales sin cancelar: {len(modals_without_cancel)}")
    
    # Guardar reporte detallado
    with open('modal_analysis_v2.txt', 'w', encoding='utf-8') as f:
        f.write("=== MODALES SIN BOTÓN CANCELAR ===\n\n")
        if modals_without_cancel:
            for modal in modals_without_cancel:
                f.write(f"Archivo: {modal['file']}\n")
                f.write(f"ID: {modal['modal_id']}\n")
                f.write(f"Título: {modal['title']}\n")
                f.write(f"Form: {'Sí' if modal['has_form'] else 'No'}\n")
                f.write(f"Submit: {'Sí' if modal['has_submit'] else 'No'}\n")
                f.write(f"Cancel: {'Sí' if modal['has_cancel'] else 'No'}\n")
                f.write("\n" + "="*50 + "\n\n")
        else:
            f.write("✅ Todos los modales tienen botón de cancelar\n")
    
    print(f"\n✅ Reporte guardado en modal_analysis_v2.txt")
    
    return modals_without_cancel

if __name__ == "__main__":
    find_modals_without_cancel_v2()
