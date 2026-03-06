import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

def create_table_proyectos():
    with get_db_cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS sys_proyectos_requerimientos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                titulo VARCHAR(255) NOT NULL,
                descripcion TEXT,
                tipo VARCHAR(50) DEFAULT 'REQUERIMIENTO', -- SUGERENCIA, PROYECTO, BUG
                estado VARCHAR(50) DEFAULT 'PENDIENTE', -- PENDIENTE, EN_PROGRESO, COMPLETADO, DESCARTADO
                prioridad VARCHAR(20) DEFAULT 'MEDIA', -- ALTA, MEDIA, BAJA
                user_id INT, -- Quien lo reportó
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # Insertar el requerimiento que pide el usuario
        c.execute("""
            INSERT INTO sys_proyectos_requerimientos 
            (enterprise_id, titulo, descripcion, tipo, estado, prioridad)
            SELECT 1, 'Tuning de Índices Restantes', 'Correr el checklist de los índices restantes para tablas no troncales que quedaron pendientes en migrations/expert_tuning.sql (apróx 300+) para cuando aumente el volumen transaccional.', 'REQUERIMIENTO', 'PENDIENTE', 'MEDIA'
            FROM DUAL
            WHERE NOT EXISTS (
                SELECT 1 FROM sys_proyectos_requerimientos WHERE titulo = 'Tuning de Índices Restantes'
            )
        """)

if __name__ == '__main__':
    create_table_proyectos()
