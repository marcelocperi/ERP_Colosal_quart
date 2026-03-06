import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        print("Creando tabla sys_enterprise_numeracion...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_enterprise_numeracion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                entidad_tipo ENUM('COMPROBANTE', 'RECIBO', 'ORDEN_PAGO', 'ORDEN_COMPRA', 'MOV_STOCK') NOT NULL,
                entidad_codigo VARCHAR(10) NOT NULL,
                punto_venta INT DEFAULT 1,
                ultimo_numero INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY (enterprise_id, entidad_tipo, entidad_codigo, punto_venta)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        # Población inicial para las empresas existentes (ID 0 por ahora)
        print("Poblando datos iniciales desde sys_tipos_comprobante...")
        
        # Obtener todas las empresas
        cursor.execute("SELECT id FROM sys_enterprises")
        enterprises = [r[0] for r in cursor.fetchall()]
        
        # Obtener todos los tipos de comprobante
        cursor.execute("SELECT codigo FROM sys_tipos_comprobante")
        tipo_comprobantes = [r[0] for r in cursor.fetchall()]
        
        count = 0
        for ent_id in enterprises:
            for cod in tipo_comprobantes:
                try:
                    cursor.execute("""
                        INSERT IGNORE INTO sys_enterprise_numeracion 
                        (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, ultimo_numero)
                        VALUES (%s, 'COMPROBANTE', %s, 1, 0)
                    """, (ent_id, cod))
                    if cursor.rowcount > 0:
                        count += 1
                except:
                    pass
        
        print(f"Población completada. {count} registros creados.")

if __name__ == '__main__':
    migrate()
