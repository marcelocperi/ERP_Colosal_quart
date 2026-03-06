from flask import Blueprint, jsonify, g
from core.decorators import login_required
from database import get_db_cursor

cm05_api_bp = Blueprint('cm05_api', __name__)

@cm05_api_bp.route('/api/terceros/<int:tercero_id>/cm05-log')
@login_required
def get_cm05_log(tercero_id):
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT log_erp_terceros_cm05.*, sys_users.username as user_name, sys_provincias.nombre as provincia_nombre
            FROM log_erp_terceros_cm05
            LEFT JOIN sys_users ON log_erp_terceros_cm05.user_action = sys_users.id
            LEFT JOIN sys_provincias ON BINARY log_erp_terceros_cm05.jurisdiccion_code = BINARY LPAD(sys_provincias.id, 3, '0')
            WHERE log_erp_terceros_cm05.tercero_id = %s
            ORDER BY log_erp_terceros_cm05.fecha_efectiva DESC
        """, (tercero_id,))
        logs = cursor.fetchall()
        
        # Format the response
        formatted_logs = []
        for l in logs:
            action_map = {0: 'Nuevo Registro', 2: 'Actualización', 3: 'Deshabilitado'}
            formatted_logs.append({
                'id_action': l['id_action'],
                'action_label': action_map.get(l['id_action'], 'Desconocido'),
                'fecha_efectiva': l['fecha_efectiva'].strftime('%Y-%m-%d %H:%M:%S') if l['fecha_efectiva'] else '',
                'user_name': l['user_name'] or 'Sistema',
                'jurisdiccion': f"{l['jurisdiccion_code']} - {l['provincia_nombre'] or 'Desconocida'}",
                'periodo_anio': l['periodo_anio'],
                'coeficiente': str(l['coeficiente']),
                'old_data': l['RECORD_JSON']
            })
            
    return jsonify(formatted_logs)
