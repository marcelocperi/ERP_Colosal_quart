import os
import json
import datetime
import requests
from database import get_db_cursor

class LogisticsService:
    """
    Servicio para interoperabilidad con organismos de control logístico (ARBA, AGIP, etc.)
    Maneja la generación y validación de COT (Código de Operación de Transporte).
    """

    @staticmethod
    async def solicitar_cot(enterprise_id, comprobante_id):
        """
        Determina si el comprobante requiere COT y lo solicita al organismo correspondiente.
        """
        async with get_db_cursor(dictionary=True) as cursor:
            # Traer datos del comprobante y cliente para evaluar reglas
            await cursor.execute("""
                SELECT c.*, t.nombre as cliente_nombre, t.cuit as cliente_cuit,
                       d.localidad, d.provincia, tc.es_logistica
                FROM erp_comprobantes c
                JOIN erp_terceros t ON c.tercero_id = t.id
                LEFT JOIN erp_direcciones d ON c.direccion_entrega_id = d.id
                JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
                WHERE c.id = %s AND c.enterprise_id = %s
            """, (comprobante_id, enterprise_id))
            comp = await cursor.fetchone()

            if not comp or not comp['es_logistica']:
                return {"success": False, "message": "El comprobante no es de tipo logístico."}

            if comp['cot']:
                return {"success": True, "cot": comp['cot'], "message": "Ya posee un COT asignado."}

            # --- REGLAS PARA ARBA (Buenos Aires) ---
            # El COT es obligatorio en PBA para transporte de bienes con valor > $X o peso > Y
            # Aquí simulamos la evaluación de reglas y el llamado al API
            provincia_destino = (comp['provincia'] or '').upper()
            es_pba = any(p in provincia_destino for p in ['BUENOS AIRES', 'PBA', 'BS AS', 'BS. AS.'])
            
            if es_pba:
                return await LogisticsService._solicitar_arba_cot_api(enterprise_id, comp)
            
            # --- REGLAS PARA AGIP (CABA) ---
            es_caba = any(p in provincia_destino for p in ['CABA', 'CIUDAD AUTONOMA', 'CAPITAL FEDERAL'])
            if es_caba:
                return await LogisticsService._solicitar_agip_cot_api(enterprise_id, comp)

            return {"success": False, "message": "No se requiere COT para este destino."}

    @staticmethod
    async def _solicitar_arba_cot_api(enterprise_id, comp):
        """
        Simulación de llamada al API de ARBA para obtener el COT.
        En producción esto enviaría un XML vía SOAP o JSON vía REST.
        """
        # Simulamos latencia de red
        import time
        time.sleep(1)
        
        # Simulación de respuesta exitosa
        # El COT suele ser un número largo alfanumérico
        import secrets
        nuevo_cot = f"ARBA-{secrets.token_hex(6).upper()}"
        
        try:
            async with get_db_cursor() as cursor:
                await cursor.execute("UPDATE erp_comprobantes SET cot = %s WHERE id = %s", (nuevo_cot, comp['id']))
            return {"success": True, "cot": nuevo_cot, "organismo": "ARBA"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    async def _solicitar_agip_cot_api(enterprise_id, comp):
        """
        Simulación de llamada al API de AGIP (CABA).
        """
        import secrets
        nuevo_cot = f"AGIP-{secrets.token_hex(6).upper()}"
        
        try:
            async with get_db_cursor() as cursor:
                await cursor.execute("UPDATE erp_comprobantes SET cot = %s WHERE id = %s", (nuevo_cot, comp['id']))
            return {"success": True, "cot": nuevo_cot, "organismo": "AGIP"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    def validar_cot(cot_nro):
        """
        Valida un COT ya existente contra los organismos oficiales.
        """
        # Simulación
        if cot_nro and len(cot_nro) > 5:
            return {"valido": True, "vencimiento": (datetime.date.today() + datetime.timedelta(days=7)).isoformat()}
        return {"valido": False, "mensaje": "Formato de COT inválido"}
