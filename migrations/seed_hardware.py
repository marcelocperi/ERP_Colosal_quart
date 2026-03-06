import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_seed():
    with get_db_cursor() as cursor:
        
        # 1. Agregar columnas driver si no existen
        logger.info("Verificando/Agregando columna 'driver'...")
        try:
            cursor.execute("ALTER TABLE stk_impresoras_config ADD COLUMN driver VARCHAR(100) DEFAULT ''")
            logger.info("Columna 'driver' agregada a stk_impresoras_config")
        except Exception as e:
            if 'Duplicate column' not in str(e):
                logger.warning(f"Error alter stk_impresoras_config: {e}")
                
        try:
            cursor.execute("ALTER TABLE stk_balanzas_config ADD COLUMN driver VARCHAR(100) DEFAULT ''")
            logger.info("Columna 'driver' agregada a stk_balanzas_config")
        except Exception as e:
            if 'Duplicate column' not in str(e):
                logger.warning(f"Error alter stk_balanzas_config: {e}")

        # Enterprise IDs
        cursor.execute("SELECT id FROM sys_enterprises")
        empresas = [e[0] for e in cursor.fetchall()]
        if not empresas:
            empresas = [1] # fallback

        # 2. Seed Impresoras (Mercado)
        impresoras_seed = [
            ("Zebra ZD421", "Zebra", "ZD421", 104, 150, "ZDesigner ZD421-203dpi ZPL"),
            ("Zebra GK420t", "Zebra", "GK420t", 104, 991, "ZDesigner GK420t"),
            ("Zebra ZT230", "Zebra", "ZT230", 104, 3988, "ZDesigner ZT230-203dpi ZPL"),
            ("Zebra GC420t", "Zebra", "GC420t", 104, 990, "ZDesigner GC420t (EPL)"),
            ("Zebra ZM400", "Zebra", "ZM400", 104, 3988, "ZDesigner ZM400-203dpi ZPL"),
            ("Epson TM-T20III", "Epson", "TM-T20III", 80, 0, "EPSON TM-T20III Receipt"),
            ("Epson TM-T88VI", "Epson", "TM-T88VI", 80, 0, "EPSON TM-T88VI Receipt"),
            ("Epson TM-U220", "Epson", "TM-U220", 76, 0, "EPSON TM-U220 Receipt"),
            ("Bixolon SRP-330II", "Bixolon", "SRP-330II", 80, 0, "Bixolon SRP-330II"),
            ("Bixolon SLP-TX400", "Bixolon", "SLP-TX400", 108, 1000, "Bixolon SLP-TX400"),
            ("Brother QL-800", "Brother", "QL-800", 62, 1000, "Brother QL-800"),
            ("Brother QL-1100", "Brother", "QL-1100", 103, 3000, "Brother QL-1100"),
            ("Honeywell PC42t", "Honeywell", "PC42t", 104, 990, "Honeywell PC42t (ZSim)"),
            ("Xprinter XP-420B", "Xprinter", "XP-420B", 108, 2286, "Xprinter XP-420B"),
            ("Xprinter XP-80C", "Xprinter", "XP-80C", 80, 0, "Xprinter XP-80C"),
            ("Rongta RP80", "Rongta", "RP80", 80, 0, "Rongta RP80 Printer"),
            ("Sato WS4", "Sato", "WS4", 104, 999, "Sato WS4 Series"),
            ("Dymo LabelWriter 450", "Dymo", "LabelWriter 450", 56, 1000, "DYMO LabelWriter 450"),
            ("Star Micronics TSP143III", "Star Micronics", "TSP100", 80, 0, "Star TSP143"),
            ("TSC TE200", "TSC", "TE200", 108, 2794, "TSC TE200"),
        ]

        logger.info("Seed de Impresoras del mercado...")
        for ent_id in empresas:
            for imp in impresoras_seed:
                cursor.execute("""
                    INSERT INTO stk_impresoras_config 
                    (enterprise_id, nombre, marca, modelo, ancho_mm, alto_mm, driver)
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    FROM DUAL
                    WHERE NOT EXISTS (
                        SELECT 1 FROM stk_impresoras_config 
                        WHERE enterprise_id = %s AND marca = %s AND modelo = %s
                    )
                """, (ent_id, imp[0], imp[1], imp[2], imp[3], imp[4], imp[5], ent_id, imp[1], imp[2]))

        # 3. Seed Balanzas (Mercado)
        balanzas_seed = [
            # ("Nombre", "Marca", "Modelo", "Tipo Conexion", "Driver")
            ("Systel Cuora", "Systel", "Cuora", "IP_RED", "Systel QUA API"),
            ("Systel Croma", "Systel", "Croma", "SERIAL_USB", "Systel Serial Bridge"),
            ("Systel Clipse", "Systel", "Clipse", "IP_RED", "Systel ITest/Net"),
            ("Systel Bumer", "Systel", "Bumer", "IP_RED", "Systel IP Driver"),
            ("Kretz Aura", "Kretz", "Aura", "IP_RED", "Kretz Integra"),
            ("Kretz Report", "Kretz", "Report", "IP_RED", "Kretz Integra NX"),
            ("Kretz Novel", "Kretz", "Novel", "SERIAL_USB", "Kretz Serial Comm"),
            ("Kretz Delta", "Kretz", "Delta", "IP_RED", "Kretz iTegra Protocol"),
            ("Digi SM-100", "Digi", "SM-100", "IP_RED", "Digi SM-C TCP"),
            ("Digi SM-320", "Digi", "SM-320", "IP_RED", "Digi SM-C TCP V2"),
            ("Digi SM-500", "Digi", "SM-500", "IP_RED", "Digi SM-C TCP HighEnd"),
            ("Toledo Ariva", "Mettler Toledo", "Ariva-S", "SERIAL_USB", "Toledo OPOS/POS for Retail"),
            ("Toledo Tiger", "Mettler Toledo", "Tiger", "IP_RED", "Toledo SPCT"),
            ("Toledo bPro", "Mettler Toledo", "bPro", "IP_RED", "Toledo bPro TCP API"),
            ("Toledo bPlus", "Mettler Toledo", "bPlus", "IP_RED", "Toledo SmartLoad"),
            ("CAS LP-1000", "CAS", "LP-1000", "SERIAL_USB", "CAS LP Serial Protocol"),
            ("CAS CL-5000", "CAS", "CL-5000", "IP_RED", "CAS CL Works"),
            ("Bizerba SC", "Bizerba", "System Class", "IP_RED", "Bizerba POS Data Protocol"),
            ("Bizerba BC II", "Bizerba", "Basic Scale", "IP_RED", "Bizerba TCP/IP"),
            ("Hasar HP-250", "Hasar", "HP-250", "IP_RED", "Hasar Scale Sync"),
        ]

        logger.info("Seed de Balanzas del mercado...")
        for ent_id in empresas:
            for b in balanzas_seed:
                cursor.execute("""
                    INSERT INTO stk_balanzas_config 
                    (enterprise_id, nombre, marca, modelo, tipo_conexion, driver)
                    SELECT %s, %s, %s, %s, %s, %s
                    FROM DUAL
                    WHERE NOT EXISTS (
                        SELECT 1 FROM stk_balanzas_config 
                        WHERE enterprise_id = %s AND marca = %s AND modelo = %s
                    )
                """, (ent_id, b[0], b[1], b[2], b[3], b[4], ent_id, b[1], b[2]))

    logger.info("Seed de hardware comercial integrado exitosamente!")

if __name__ == '__main__':
    run_seed()
