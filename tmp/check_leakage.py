import mysql.connector

def check_enterprise_leakage():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="biblioteca"
        )
        cursor = conn.cursor()
        
        # Core tables that should only have enterprise_id = 0 as templates
        tables = [
            'cont_plan_cuentas',
            'sys_tipos_comprobante',
            'stk_mov_motivos',
            'stk_articulo_naturalezas'
        ]
        
        for t in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {t} WHERE enterprise_id != 0")
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"ALERTA: Tabla {t} tiene {count} registros con enterprise_id != 0")
                # Optionally delete or report
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_enterprise_leakage()
