
def validar_cuit(cuit: str) -> bool:
    """
    Valida un número de CUIT/CUIL argentino siguiendo la regla del Módulo 11.
    
    1. XX-XXXXXXXX-Y (Tipo-DNI-Verificador).
    2. Eliminar guiones: Trabajar con los 11 dígitos numéricos.
    3. Multiplicación: Multiplicar cada uno de los primeros 10 dígitos por 5, 4, 3, 2, 7, 6, 5, 4, 3, 2.
    4. Sumatoria: Sumar los 10 resultados.
    5. Módulo 11: resto = sumatoria % 11.
    6. Cálculo del Verificador:
       - Si resto == 0 -> Verificador = 0.
       - Si resto == 1 -> Verificador = 9 (casos especiales 23, 33, 34).
       - Si resto > 1 -> Verificador = 11 - resto.
    7. Comparación: Verificar si coincide con el dígito 11.
    """
    if not cuit:
        return False
        
    # Limpiar caracteres no numéricos
    digits = "".join(filter(str.isdigit, str(cuit)))
    
    if len(digits) != 11:
        return False
        
    serie = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2]
    sumatoria = 0
    
    # Multiplicación y Sumatoria
    for i in range(10):
        sumatoria += int(digits[i]) * serie[i]
        
    resto = sumatoria % 11
    
    if resto == 0:
        verificador_calculado = 0
    elif resto == 1:
        verificador_calculado = 9
    else:
        verificador_calculado = 11 - resto
        
    return verificador_calculado == int(digits[10])

def clean_cuit(cuit: str) -> str:
    """Elimina guiones y cualquier carácter no numérico"""
    if not cuit: return ""
    return "".join(filter(str.isdigit, str(cuit)))

def format_cuit(cuit: str) -> str:
    """Añade guiones al formato XX-XXXXXXXX-Y"""
    digits = clean_cuit(cuit)
    if not digits: return ""
    if len(digits) != 11: return digits # Devolver tal cual si no es formato CUIT
    return f"{digits[:2]}-{digits[2:10]}-{digits[10:]}"
