import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

def run_all_tuning():
    path = os.path.join(os.path.dirname(__file__), 'expert_tuning.sql')
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    cmds_to_run = []
    current_table = None
    for line in lines:
        if line.startswith('-- 📦 TABLA:'):
            current_table = line.split(':')[1].strip()
        elif line.startswith('CREATE INDEX'):
            cmds_to_run.append((current_table, line.strip()))

    success_count = 0
    skip_count = 0
    error_count = 0

    with get_db_cursor() as c:
        for t, cmd in cmds_to_run:
            try:
                c.execute(cmd)
                success_count += 1
            except Exception as e:
                if "Duplicate key name" in str(e):
                    skip_count += 1
                else:
                    error_count += 1
                    print(f"❌ Error en {t} -> {cmd} : {e}")

        # Marcar la tarea como completada
        c.execute("""
            UPDATE sys_proyectos_requerimientos 
            SET estado = 'COMPLETADO', descripcion = CONCAT(descripcion, '\n\n[SISTEMA]: Ejecutado masivamente ', NOW())
            WHERE titulo = 'Tuning de Índices Restantes'
        """)

    print(f"✅ Ejecución Finalizada. Creados: {success_count} | Omitidos por preexistentes: {skip_count} | Errores: {error_count}")

if __name__ == '__main__':
    run_all_tuning()
