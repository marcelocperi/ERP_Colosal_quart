"""
AI Chat Routes — CORE AUDITOR
Rutas para la pantalla de chat con el LLM local (Ollama / TinyLlama).

El ranking (0-10) se persiste en `sys_ai_feedback` junto con:
  - pregunta del usuario
  - respuesta del LLM
  - score asignado
  - usuario y empresa
Esto permite análisis posterior de calidad y mejora de prompts.
"""

from flask import Blueprint, render_template, request, jsonify, g
from core.decorators import login_required
from services.local_intelligence_service import LocalIntelligenceService
from database import get_db_cursor
import logging
import datetime

logger = logging.getLogger(__name__)

ai_chat_bp = Blueprint('ai_chat', __name__, template_folder='../core/templates')


# ─────────────────────────────────────────────
#  PÁGINA PRINCIPAL DEL CHAT
# ─────────────────────────────────────────────
@ai_chat_bp.route('/ai/chat', methods=['GET', 'POST'])
@login_required
def ai_chat_page():
    """Renderiza la pantalla de chat con el CORE AUDITOR (Técnico)."""
    ollama_online = LocalIntelligenceService.check_health()
    response_text = None
    question = None

    if request.method == 'POST':
        question = request.form.get('question')
        if question and ollama_online:
            user_context = f"Usuario: {g.user['username']} | Rol: {g.user['role_name']} | Auditoría Técnica"
            result = LocalIntelligenceService.consult_rules(question, user_context=user_context)
            response_text = result.get('response') if 'error' not in result else f"Error: {result['error']}"
            if response_text:
                _save_interaction(question, response_text)

    return render_template('admin_ai_auditor.html', 
                          ollama_status=ollama_online, 
                          response=response_text, 
                          question=question)


@ai_chat_bp.route('/ai/assistant')
@login_required
def ai_user_assistant():
    """Renderiza la pantalla de chat para el usuario general (Visual/Premium)."""
    return render_template('ai_chat.html')


# ─────────────────────────────────────────────
#  API: HEALTH CHECK
# ─────────────────────────────────────────────
@ai_chat_bp.route('/api/ai/health')
@login_required
def ai_health():
    """Verifica si Ollama está disponible."""
    online = LocalIntelligenceService.check_health()
    return jsonify({'online': online})


# ─────────────────────────────────────────────
#  API: CHAT — Procesa el mensaje
# ─────────────────────────────────────────────
@ai_chat_bp.route('/api/ai/chat', methods=['POST'])
@login_required
def ai_chat():
    """
    Recibe { message: str } y retorna { response: str, session_id: int }.
    También persiste la interacción en sys_ai_feedback (sin rating aún).
    """
    data = request.get_json(silent=True) or {}
    question = (data.get('message') or '').strip()

    if not question:
        return jsonify({'error': 'Mensaje vacío'}), 400
    if len(question) > 2000:
        return jsonify({'error': 'Mensaje demasiado largo (máx. 2000 caracteres)'}), 400

    # Contexto operativo del usuario para enriquecer el prompt
    user_context = (
        f"Usuario: {g.user['username']} | "
        f"Rol: {g.user['role_name']} | "
        f"Empresa: {g.user.get('enterprise_id', 'N/A')}"
    )

    # Capturar historial de la conversación si existe para permitir 'paginación' (continuar donde quedó)
    history = data.get('history') or []

    # Llamar al LLM con soporte de historial
    result = LocalIntelligenceService.consult_rules(question, user_context=user_context, history=history)

    if 'error' in result:
        return jsonify({'error': result['error']}), 503

    response_text = result.get('response', '')

    # Persistir la interacción (rating queda NULL hasta que el usuario califica)
    session_id = _save_interaction(question, response_text)

    return jsonify({
        'response': response_text,
        'session_id': session_id   # El frontend lo usa para enviar el rating luego
    })


# ─────────────────────────────────────────────
#  API: RATING — Guarda el ranking del usuario
# ─────────────────────────────────────────────
@ai_chat_bp.route('/api/ai/rate', methods=['POST'])
@login_required
def ai_rate():
    """
    Recibe { session_id: int, rating: int (0-10) } y actualiza el rating
    en sys_ai_feedback. Si no hay session_id guarda un registro libre.

    El rating es útil porque:
    - Permite identificar respuestas de baja calidad y revisar el prompt.
    - Genera estadísticas por usuario, rol y tipo de pregunta.
    - Puede usarse para fine-tuning del modelo en el futuro.
    """
    data = request.get_json(silent=True) or {}
    session_id = data.get('session_id')
    rating = data.get('rating')
    question = data.get('question', '')
    response = data.get('response', '')

    # Validar rating
    try:
        rating = int(rating)
        if not (0 <= rating <= 10):
            raise ValueError()
    except (TypeError, ValueError):
        return jsonify({'error': 'Rating inválido (debe ser 0-10)'}), 400

    try:
        with get_db_cursor() as cursor:
            if session_id:
                # Actualizar registro existente
                cursor.execute("""
                    UPDATE sys_ai_feedback
                    SET rating = %s,
                        rated_at = %s,
                        rating_label = %s
                    WHERE id = %s AND enterprise_id = %s AND user_id = %s
                """, (
                    rating,
                    datetime.datetime.now(),
                    _rating_label(rating),
                    session_id,
                    g.user['enterprise_id'],
                    g.user['id']
                ))
            else:
                # Crear registro si el frontend no envió session_id
                cursor.execute("""
                    INSERT INTO sys_ai_feedback
                        (enterprise_id, user_id, question, response, rating, rating_label, rated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    g.user['enterprise_id'],
                    g.user['id'],
                    question[:2000],
                    response[:8000],
                    rating,
                    _rating_label(rating),
                    datetime.datetime.now()
                ))

        return jsonify({'ok': True, 'rating': rating, 'label': _rating_label(rating)})

    except Exception as e:
        logger.error(f"AI RATE ERROR: {e}")
        return jsonify({'error': str(e)}), 500


# ─────────────────────────────────────────────
#  API: STATS — Estadísticas de feedback
# ─────────────────────────────────────────────
@ai_chat_bp.route('/api/ai/stats')
@login_required
def ai_stats():
    """
    Devuelve estadísticas del feedback para la empresa actual:
    - Total de interacciones
    - Rating promedio
    - Distribución de ratings
    - Peores respuestas (rating <= 3) para revisión
    """
    try:
        with get_db_cursor(dictionary=True) as cursor:
            ent_id = g.user['enterprise_id']

            # Totales generales
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    AVG(rating) as avg_rating,
                    SUM(CASE WHEN rating <= 3 THEN 1 ELSE 0 END) as bad_count,
                    SUM(CASE WHEN rating >= 8 THEN 1 ELSE 0 END) as good_count
                FROM sys_ai_feedback
                WHERE enterprise_id = %s AND rating IS NOT NULL
            """, (ent_id,))
            summary = cursor.fetchone()

            # Peores respuestas para revisión (rating <= 3)
            cursor.execute("""
                SELECT id, question, response, rating, rating_label, rated_at,
                       u.username
                FROM sys_ai_feedback f
                LEFT JOIN sys_users u ON f.user_id = u.id
                WHERE f.enterprise_id = %s AND f.rating <= 3 AND f.rating IS NOT NULL
                ORDER BY f.rated_at DESC
                LIMIT 10
            """, (ent_id,))
            worst = cursor.fetchall()

            # Distribución de ratings (histograma 0-10)
            cursor.execute("""
                SELECT rating, COUNT(*) as cnt
                FROM sys_ai_feedback
                WHERE enterprise_id = %s AND rating IS NOT NULL
                GROUP BY rating ORDER BY rating
            """, (ent_id,))
            distribution = {row['rating']: row['cnt'] for row in cursor.fetchall()}

        return jsonify({
            'summary': {
                'total': summary['total'] if summary else 0,
                'avg_rating': round(float(summary['avg_rating']), 1) if summary and summary['avg_rating'] else None,
                'bad_count': summary['bad_count'] if summary else 0,
                'good_count': summary['good_count'] if summary else 0,
            },
            'worst_responses': worst,
            'distribution': distribution
        })

    except Exception as e:
        logger.error(f"AI STATS ERROR: {e}")
        return jsonify({'error': str(e)}), 500


# ─────────────────────────────────────────────
#  HELPERS PRIVADOS
# ─────────────────────────────────────────────
def _save_interaction(question: str, response: str) -> int | None:
    """
    Guarda la interacción pregunta/respuesta en sys_ai_feedback.
    Retorna el ID del registro para poder vincular el rating después.
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO sys_ai_feedback
                    (enterprise_id, user_id, question, response, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                g.user['enterprise_id'],
                g.user['id'],
                question[:2000],
                response[:8000],
                datetime.datetime.now()
            ))
            return cursor.lastrowid
    except Exception as e:
        logger.warning(f"No se pudo guardar interacción AI: {e}")
        return None


def _rating_label(rating: int) -> str:
    """Clasifica el rating en etiquetas legibles para análisis."""
    if rating <= 2:   return 'PÉSIMO'
    if rating <= 4:   return 'BAJO'
    if rating == 5:   return 'ACEPTABLE'
    if rating <= 7:   return 'BUENO'
    if rating <= 9:   return 'MUY BUENO'
    return 'EXCELENTE'
