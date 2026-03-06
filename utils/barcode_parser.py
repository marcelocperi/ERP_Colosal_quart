import re
from decimal import Decimal

def parse_dynamic_barcode(code, enterprise_id, cursor):
    """
    Intenta parsear un código de barras dinámico (ej: EAN-13 de balanza).
    
    Estructura típica EAN-13 Balanza:
    - 21 00005 01250 8
    - [Pr] [PLU] [Peso] [C]
    
    Retorna un diccionario con:
    - is_dynamic: True/False
    - sku_plu: Código limpio del artículo
    - valor: El valor extraído (peso, unidades o precio)
    - tipo: 'PESO', 'PRECIO', 'CANTIDAD'
    """
    if not code or len(code) < 12:
        return None

    # Buscar reglas activas para esta empresa
    cursor.execute("""
        SELECT prefijo, tipo_valor, pos_prod_inicio, pos_prod_fin, pos_val_inicio, pos_val_fin, divisor
        FROM stk_barcode_rules
        WHERE enterprise_id = %s AND activo = 1
    """, (enterprise_id,))
    rules = cursor.fetchall()

    for rule in rules:
        prefijo, tipo_valor, p_prod_ini, p_prod_fin, p_val_ini, p_val_fin, divisor = rule
        
        if code.startswith(prefijo):
            try:
                # Extraer SKU/PLU (usualmente dígitos 2 a 7)
                sku_raw = code[p_prod_ini:p_prod_fin]
                # Limpiar ceros a la izquierda (si es numérico)
                sku_clean = str(int(sku_raw)) 
                
                # Extraer Valor (usualmente dígitos 7 a 12)
                val_raw = code[p_val_ini:p_val_fin]
                val_num = Decimal(val_raw)
                
                valor_final = (val_num / Decimal(str(divisor))).quantize(Decimal('0.001'))

                return {
                    'is_dynamic': True,
                    'sku_plu': sku_clean,
                    'sku_raw': sku_raw,
                    'valor': float(valor_final),
                    'tipo': tipo_valor,
                    'regla': prefijo
                }
            except Exception:
                continue

    return None
