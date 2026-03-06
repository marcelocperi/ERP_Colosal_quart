import json

def get_incompatibility_alerts(incoming_safety, existing_items_safety):
    """
    Compara un artículo entrante con una lista de artículos ya existentes en una ubicación.
    incoming_safety: dict con campos de stk_articulos_seguridad + 'nombre_comun'
    existing_items_safety: lista de dicts similares.
    """
    alerts = []
    
    # Extraer datos del entrante
    inc_id = incoming_safety.get('articulo_id')
    inc_class = str(incoming_safety.get('clase_riesgo') or '')
    inc_pictos = incoming_safety.get('pictogramas_json') or []
    inc_nombre = incoming_safety.get('nombre_comun') or f"Art. {inc_id}"
    
    for item in existing_items_safety:
        # No compararse consigo mismo
        if item.get('articulo_id') == inc_id:
            continue
            
        item_id = item.get('articulo_id')
        item_class = str(item.get('clase_riesgo') or '')
        item_pictos = item.get('pictogramas_json') or []
        item_nombre = item.get('nombre_comun') or f"Art. {item_id}"
        
        # --- REGLA 1: INFLAMABLES vs OXIDANTES (Peligro de incendio/explosión) ---
        # GHS02 (Inflamable) / GHS03 (Oxidante/Comburente)
        is_inc_flam = 'GHS02' in inc_pictos or inc_class == '3'
        is_inc_oxid = 'GHS03' in inc_pictos or inc_class in ['5.1', '5.2']
        
        is_item_flam = 'GHS02' in item_pictos or item_class == '3'
        is_item_oxid = 'GHS03' in item_pictos or item_class in ['5.1', '5.2']
        
        if (is_inc_flam and is_item_oxid) or (is_inc_oxid and is_item_flam):
            alerts.append({
                'severity': 'DANGER',
                'message': f"RIESGO DE EXPLOSIÓN: {inc_nombre} ({'Inflamable' if is_inc_flam else 'Oxidante'}) es incompatible con {item_nombre} ({'Oxidante' if is_item_oxid else 'Inflamable'}).",
                'affected_id': item_id
            })

        # --- REGLA 2: CORROSIVOS vs INFLAMABLES (Daño a contenedores) ---
        # GHS05 (Corrosivo) / Clase 8
        is_inc_corr = 'GHS05' in inc_pictos or inc_class == '8'
        is_item_corr = 'GHS05' in item_pictos or item_class == '8'
        
        if (is_inc_corr and is_item_flam) or (is_inc_flam and is_item_corr):
            alerts.append({
                'severity': 'WARNING',
                'message': f"ALERTA DE SEGREGACIÓN: {inc_nombre} y {item_nombre}. Los corrosivos y líquidos inflamables deben almacenarse con separación física.",
                'affected_id': item_id
            })
            
        # --- REGLA 3: CORROSIVOS vs OXIDANTES ---
        if (is_inc_corr and is_item_oxid) or (is_inc_oxid and is_item_corr):
             alerts.append({
                'severity': 'DANGER',
                'message': f"INCOMPATIBILIDAD QUÍMICA: {inc_nombre} y {item_nombre}. Sustancias Corrosivas y Comburentes pueden generar reacciones exotérmicas peligrosas.",
                'affected_id': item_id
            })

        # --- REGLA 4: EXPLOSIVOS (GHS01 / Clase 1) - Aislamiento Total ---
        is_inc_expl = 'GHS01' in inc_pictos or inc_class == '1'
        is_item_expl = 'GHS01' in item_pictos or item_class == '1'
        
        if is_inc_expl or is_item_expl:
            # Si uno es explosivo, el otro tiene que ser compatible (generalmente nada lo es en el mismo rack)
            alerts.append({
                'severity': 'DANGER',
                'message': f"SEGURIDAD CRÍTICA: {inc_nombre} o {item_nombre} es un EXPLOSIVO. Requiere polvorín exclusivo o segregación máxima según normativa local.",
                'affected_id': item_id
            })

    return alerts
