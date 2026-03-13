from database import get_db_cursor
import logging

logger = logging.getLogger(__name__)

class NumerationService:
    @staticmethod
    async def get_next_number(enterprise_id, entidad_tipo, entidad_codigo, punto_venta=1):
        """
        Calcula el próximo número para un comprobante o entidad.
        Considera tanto el último número registrado en la tabla de parámetros 
        como el máximo número real en la tabla de await comprobantes(para evitar duplicados).
        """
        async with get_db_cursor(dictionary=True) as cursor:
            # 1. Verificar si el tipo es numerable en el maestro
            if entidad_tipo == 'COMPROBANTE':
                await cursor.execute("SELECT es_numerable FROM sys_tipos_comprobante WHERE codigo = %s", (entidad_codigo,))
                t_row = await cursor.fetchone()
                if t_row and t_row['es_numerable'] == 0:
                    return 0 # No es numerable por configuración de tipo

            # 2. Obtener base desde parámetros
            await cursor.execute("""
                SELECT ultimo_numero 
                FROM sys_enterprise_numeracion 
                WHERE enterprise_id = %s AND entidad_tipo = %s AND entidad_codigo = %s AND punto_venta = %s
            """, (enterprise_id, entidad_tipo, entidad_codigo, punto_venta))
            row_p = await cursor.fetchone()
            base_p = row_p['ultimo_numero'] if row_p else 0

            # 3. Verificar contra comprobantes reales (solo si es tipo COMPROBANTE)
            base_r = 0
            if entidad_tipo == 'COMPROBANTE':
                await cursor.execute("""
                    SELECT MAX(numero) as max_n 
                    FROM erp_comprobantes 
                    WHERE enterprise_id = %s AND tipo_comprobante = %s AND punto_venta = %s
                      AND es_numerable = 1
                """, (enterprise_id, entidad_codigo, punto_venta))
                row_r = await cursor.fetchone()
                base_r = row_r['max_n'] if row_r and row_r['max_n'] else 0

            next_n = max(base_p, base_r) + 1
            return next_n

    @staticmethod
    async def update_last_number(enterprise_id, entidad_tipo, entidad_codigo, punto_venta, numero):
        """
        Actualiza el último número generado en la tabla de parámetros.
        """
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                UPDATE sys_enterprise_numeracion 
                SET ultimo_numero = %s 
                WHERE enterprise_id = %s AND entidad_tipo = %s AND entidad_codigo = %s AND punto_venta = %s
            """, (numero, enterprise_id, entidad_tipo, entidad_codigo, punto_venta))
            
            # Si no existía (raro), insertarlo
            if cursor.rowcount == 0:
                await cursor.execute("""
                    INSERT IGNORE INTO sys_enterprise_numeracion 
                    (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, ultimo_numero)
                    VALUES (%s, %s, %s, %s, %s)
                """, (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, numero))
