#!/usr/bin/env python3
"""
Migración: Seguimiento de Buques (Vessel Tracking)
Crea la tabla imp_vessel_tracking para almacenar el historial de rastreo AIS.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from database import get_db_cursor
    print("[OK] database importada")
except ImportError as e:
    print(f"[ERROR] {e}"); sys.exit(1)

SQL_STEPS = [
    ("imp_vessel_tracking: CREATE", """
        CREATE TABLE IF NOT EXISTS imp_vessel_tracking (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            enterprise_id       INT NOT NULL,
            orden_compra_id     INT NOT NULL,
            vessel_mmsi         VARCHAR(20) DEFAULT NULL COMMENT 'ID único de AIS del buque',
            vessel_name         VARCHAR(100) DEFAULT NULL,
            last_lat            DECIMAL(10, 8) DEFAULT NULL,
            last_lon            DECIMAL(11, 8) DEFAULT NULL,
            eta_predicted       DATETIME DEFAULT NULL,
            vessel_status       VARCHAR(50) DEFAULT NULL,
            last_data_received  DATETIME DEFAULT NULL,
            raw_json            TEXT DEFAULT NULL,
            user_id             INT DEFAULT NULL,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_track_orden (orden_compra_id),
            INDEX idx_track_ent (enterprise_id)
        )
    """)
]

def run():
    print("\n" + "="*60)
    print("  MIGRACIÓN: Tracking de Buques (AIS)")
    print("="*60)
    with get_db_cursor() as cursor:
        for label, sql in SQL_STEPS:
            try:
                cursor.execute(sql.strip())
                print(f"  [✓] {label}")
            except Exception as e:
                print(f"  [✗] {label}: {e}")
                return False
    print("="*60 + "\n")
    return True

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
