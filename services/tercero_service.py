from database import get_db_cursor
import logging

logger = logging.getLogger(__name__)

class TerceroService:
    @staticmethod
    def get_proveedores_for_selector(enterprise_id):
        """
        Retorna la lista de proveedores con todos los campos necesarios 
        para los selectores inteligentes (Select2).
        """
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT 
                    t.id, 
                    t.codigo, 
                    t.nombre, 
                    t.cuit, 
                    d.localidad,
                    t.email
                FROM erp_terceros t
                LEFT JOIN erp_direcciones d ON t.id = d.tercero_id AND d.es_fiscal = 1
                WHERE (t.enterprise_id = %s OR t.enterprise_id = 0) 
                  AND t.es_proveedor = 1 
                  AND t.activo = 1 
                ORDER BY t.nombre
            """, (enterprise_id,))
            return cursor.fetchall()

    @staticmethod
    def generar_siguiente_codigo(enterprise_id, tipo):
        """
        Genera el siguiente código secuencial para un tercero.
        tipo: 'CLI', 'PRO', 'EMP', etc.
        """
        prefix = tipo[:3].upper()
        if len(prefix) < 3: prefix = (prefix + "XXX")[:3]
        
        with get_db_cursor(dictionary=True) as cursor:
            # Buscar el código más alto que coincida con el prefijo
            # Usamos una expresión regular para asegurar que termina en números si es posible
            cursor.execute("""
                SELECT codigo 
                FROM erp_terceros 
                WHERE enterprise_id = %s 
                AND codigo LIKE %s 
                ORDER BY codigo DESC 
                LIMIT 1
            """, (enterprise_id, f"{prefix}%"))
            result = cursor.fetchone()
            
            if result and result['codigo']:
                import re
                # Extraer la parte numérica al final
                match = re.search(r'(\d+)$', result['codigo'])
                if match:
                    last_num = int(match.group(1))
                    next_num = last_num + 1
                    return f"{prefix}{next_num:05d}"
            
            return f"{prefix}00001"

    @staticmethod
    def get_terceros_generales(enterprise_id):
        """Clientes y proveedores para movimientos generales."""
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT id, codigo, nombre, cuit, localidad, es_cliente, es_proveedor
                FROM erp_terceros
                WHERE enterprise_id = %s AND activo = 1
                ORDER BY nombre
            """, (enterprise_id,))
            return cursor.fetchall()
