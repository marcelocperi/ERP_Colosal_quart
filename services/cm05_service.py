import json
from datetime import datetime
from database import get_db_cursor

class CM05Service:
    @staticmethod
    async def log_action(cursor, enterprise_id, tercero_id, jurisdiccion, periodo_anio, coeficiente, user_id, action_id, es_cliente, es_proveedor, old_data_json):
        # action_id: 0 = insert, 2 = update, 3 = delete/deshabilitar
        await cursor.execute("""
            INSERT INTO log_erp_terceros_cm05 (id_action, fecha_efectiva, user_action, tercero_id, es_cliente, es_proveedor, jurisdiccion_code, periodo_anio, coeficiente, RECORD_JSON)
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        """, (action_id, user_id, tercero_id, es_cliente, es_proveedor, jurisdiccion, periodo_anio, coeficiente, old_data_json))

    @staticmethod
    async def upsert_coeficiente(enterprise_id, tercero_id, jurisdiccion, periodo_anio, coeficiente, user_id):
        async with get_db_cursor(dictionary=True) as cursor:
            # Check if exists
            await cursor.execute("SELECT id, user_insert, date_insert, user_update, date_update, coeficiente FROM erp_terceros_cm05 WHERE tercero_id = %s AND enterprise_id = %s AND jurisdiccion_code = %s AND periodo_anio = %s", (tercero_id, enterprise_id, jurisdiccion, periodo_anio))
            exists = await cursor.fetchone()
            
            # Identify if client or supplier
            await cursor.execute("SELECT es_cliente, es_proveedor FROM erp_terceros WHERE id = %s", (tercero_id,))
            tercero = await cursor.fetchone()
            es_cliente = tercero['es_cliente'] if tercero else 0
            es_proveedor = tercero['es_proveedor'] if tercero else 0
            
            if exists:
                old_data_json = json.dumps(exists, default=str)
                # Apply Update
                await cursor.execute("""
                    UPDATE erp_terceros_cm05 
                    SET coeficiente = %s, user_id_update = %s, date_update = NOW()
                    WHERE id = %s
                """, (coeficiente, user_id, exists['id']))
                # Action 2 = update
                await CM05Service.log_action(cursor, enterprise_id, tercero_id, jurisdiccion, periodo_anio, coeficiente, user_id, 2, es_cliente, es_proveedor, old_data_json)
            else:
                # Apply Insert
                await cursor.execute("""
                    INSERT INTO erp_terceros_cm05 (enterprise_id, tercero_id, jurisdiccion_code, periodo_anio, coeficiente, user_id, date_insert)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (enterprise_id, tercero_id, jurisdiccion, periodo_anio, coeficiente, user_id))
                # Action 0 = insert
                await CM05Service.log_action(cursor, enterprise_id, tercero_id, jurisdiccion, periodo_anio, coeficiente, user_id, 0, es_cliente, es_proveedor, "{}")

    @staticmethod
    async def delete_coeficiente(enterprise_id, item_id, user_id):
         async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT * FROM erp_terceros_cm05 WHERE id = %s AND enterprise_id = %s", (item_id, enterprise_id))
            exists = await cursor.fetchone()
            if not exists: return
            
            await cursor.execute("SELECT es_cliente, es_proveedor FROM erp_terceros WHERE id = %s", (exists['tercero_id'],))
            tercero = await cursor.fetchone()
            es_cliente = tercero['es_cliente'] if tercero else 0
            es_proveedor = tercero['es_proveedor'] if tercero else 0
            
            old_data_json = json.dumps(exists, default=str)
            
            # Delete 
            await cursor.execute("DELETE FROM erp_terceros_cm05 WHERE id = %s", (item_id,))
            
            # Action 3 = delete/deshabilitar
            await CM05Service.log_action(cursor, enterprise_id, exists['tercero_id'], exists['jurisdiccion_code'], exists['periodo_anio'], exists['coeficiente'], user_id, 3, es_cliente, es_proveedor, old_data_json)
