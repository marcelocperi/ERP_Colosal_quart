import os
import sys
import datetime
import json
import logging
import argparse
import asyncio

# Añadir el directorio raíz al path para importar módulos locales
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import get_db_cursor
from services.afip_service import AfipService

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

async def update_enterprise_padron(enterprise_id, cron_log_id=None):
    """
    Actualiza los datos de AFIP para todos los proveedores y clientes de una empresa.
    """
    summary = {
        "procesados": 0,
        "exitosos": 0,
        "errores": 0,
        "detalles": []
    }

    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Obtener todos los terceros con CUIT válido
            cursor.execute("""
                SELECT id, cuit, nombre 
                FROM erp_terceros 
                WHERE enterprise_id = %s AND cuit IS NOT NULL AND cuit != ''
            """, (enterprise_id,))
            terceros = cursor.fetchall()
            
            logger.info(f"Empresa {enterprise_id}: Procesando {len(terceros)} terceros")
            
            for t in terceros:
                summary["procesados"] += 1
                # Limpiar CUIT (AFIP requiere solo números)
                cuit_digits = "".join(filter(str.isdigit, str(t['cuit'])))
                
                if len(cuit_digits) != 11:
                    summary["detalles"].append(f"CUIT Inválido: {t['nombre']} ({t['cuit']})")
                    summary["errores"] += 1
                    continue
                
                # 2. Consultar AFIP (AWAITED - CORRECTED)
                res = await AfipService.consultar_padron(enterprise_id, cuit_digits)
                
                if res['success']:
                    data = res['data']
                    # 3. Actualizar base de datos
                    # Usamos 'nombre' como campo de razón social
                    cursor.execute("""
                        UPDATE erp_terceros 
                        SET nombre = %s, 
                            afip_last_check = %s,
                            afip_data = %s
                        WHERE id = %s
                    """, (
                        data['razon_social'], 
                        datetime.datetime.now(), 
                        json.dumps(data),
                        t['id']
                    ))
                    summary["exitosos"] += 1
                else:
                    summary["errores"] += 1
                    summary["detalles"].append(f"Error CUIT {cuit_digits}: {res.get('error')}")

            cursor.connection.commit()
            
    except Exception as e:
        logger.error(f"Error crítico en empresa {enterprise_id}: {str(e)}")
        summary["detalles"].append(f"ERROR CRITICO: {str(e)}")

    return summary

async def run_cron(target_enterprise_id=None):
    """
    Función principal llamada por el motor de crons.
    """
    cron_name = "Actualización Automática Padrón Federal"
    
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Identificar el CRON en la tabla sys_crons para la empresa solicitada
        query_ent = target_enterprise_id if target_enterprise_id is not None else 0
        cursor.execute("SELECT id FROM sys_crons WHERE nombre = %s AND enterprise_id = %s", (cron_name, query_ent))
        cron = cursor.fetchone()
        
        if not cron:
            cursor.execute("SELECT id FROM sys_crons WHERE nombre = %s AND enterprise_id = 0", (cron_name,))
            cron = cursor.fetchone()

        if not cron:
            logger.error(f"No se encontró la definición del cron '{cron_name}' en la base de datos.")
            return

        # 2. Crear Log de inicio
        cursor.execute("""
            INSERT INTO sys_crons_logs (cron_id, fecha_inicio, status, resultado)
            VALUES (%s, %s, %s, %s)
        """, (cron['id'], datetime.datetime.now(), 'exito', 'Iniciando proceso...'))
        log_id = cursor.lastrowid
        cursor.connection.commit()

    # 3. Determinar qué empresas procesar
    enterprises_to_process = []
    with get_db_cursor(dictionary=True) as cursor:
        if target_enterprise_id is not None:
            cursor.execute("SELECT id, nombre FROM sys_enterprises WHERE id = %s", (target_enterprise_id,))
        else:
            cursor.execute("SELECT id, nombre FROM sys_enterprises")
        enterprises_to_process = cursor.fetchall()
        
    all_results = []
    for emp in enterprises_to_process:
        res = await update_enterprise_padron(emp['id'], log_id)
        if res['procesados'] > 0 or target_enterprise_id is not None:
            all_results.append({
                "empresa": emp['nombre'],
                "enterprise_id": emp['id'],
                "procesados": res['procesados'],
                "ok": res['exitosos'],
                "err": res['errores'],
                "errores_detalle": res['detalles'][:15]
            })

    # 4. Finalizar Log
    end_time = datetime.datetime.now()
    status = 'exito'
    resultado = json.dumps(all_results, indent=2, ensure_ascii=False)

    from database import calcular_proxima_ejecucion
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT frecuencia, planificacion FROM sys_crons WHERE id = %s", (cron['id'],))
        cron_row = cursor.fetchone()
    proxima = calcular_proxima_ejecucion(cron_row['frecuencia'], cron_row['planificacion']) if cron_row else None

    with get_db_cursor() as cursor:
        cursor.execute("""
            UPDATE sys_crons_logs 
            SET fecha_fin = %s, status = %s, resultado = %s
            WHERE id = %s
        """, (end_time, status, resultado, log_id))

        cursor.execute(
            "UPDATE sys_crons SET ultima_ejecucion = %s, proxima_ejecucion = %s WHERE id = %s",
            (end_time, proxima, cron['id'])
        )
        cursor.connection.commit()

    logger.info(f"Cron finalizado con éxito para {len(enterprises_to_process)} empresas.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Actualización masiva de Padrón AFIP')
    parser.add_argument('--enterprise_id', type=int, help='ID de la empresa específica a procesar')
    args = parser.parse_args()
    
    asyncio.run(run_cron(args.enterprise_id))
