from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        cursor.execute("DESCRIBE erp_terceros")
        cols = [r[0] for r in cursor.fetchall()]
        
        new_cols = [
            ("condicion_pago_id", "INT NULL"),
            ("condicion_pago_pendiente_id", "INT NULL"),
            ("estado_aprobacion_pago", "ENUM('APROBADO', 'PENDIENTE', 'RECHAZADO') DEFAULT 'APROBADO'"),
            ("id_gerente_aprobador", "INT NULL"),
            ("fecha_aprobacion_pago", "DATETIME NULL")
        ]
        
        for col_name, col_def in new_cols:
            if col_name not in cols:
                print(f"Adding {col_name}...")
                cursor.execute(f"ALTER TABLE erp_terceros ADD COLUMN {col_name} {col_def}")
            else:
                print(f"Column {col_name} already exists.")

if __name__ == "__main__":
    migrate()
