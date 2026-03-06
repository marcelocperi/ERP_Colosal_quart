import sys
sys.path.append('multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    # Obtener todas las empresas que son monotributistas
    cursor.execute("SELECT id FROM sys_enterprises WHERE condicion_iva LIKE '%Monotributo%' OR condicion_iva LIKE '%Monotributista%'")
    ids = [r['id'] for r in cursor.fetchall()]
    
    if ids:
        # Actualizar sus comprobantes
        ids_str = ",".join(map(str, ids))
        cursor.execute(f"UPDATE erp_comprobantes SET tipo_comprobante = '011' WHERE tipo_comprobante = '001' AND enterprise_id IN ({ids_str})")
        cursor.execute(f"UPDATE erp_comprobantes SET tipo_comprobante = '013' WHERE tipo_comprobante = '003' AND enterprise_id IN ({ids_str})")
        cursor.execute(f"UPDATE erp_comprobantes SET tipo_comprobante = '012' WHERE tipo_comprobante = '002' AND enterprise_id IN ({ids_str})")
        print(f"Limpieza masiva completada para empresas Monotributistas: {ids}")
    else:
        print("No se encontraron empresas Monotributistas para limpiar.")
