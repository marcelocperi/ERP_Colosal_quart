
import requests
import os
import re
import logging

logger = logging.getLogger(__name__)


class LocalIntelligenceService:
    """
    Motor de Inteligencia Local para Colosal ERP.
    Versión paginada: detecta el tema de la pregunta y carga solo
    el chunk de reglas relevante, optimizando los 512 tokens disponibles.
    """
    OLLAMA_URL      = "http://localhost:11434/api/generate"
    OLLAMA_TAGS_URL = "http://localhost:11434/api/tags"
    RULES_DIR       = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".brain", "rules")
    DEFAULT_MODEL   = "tinyllama:latest"

    # ── Ventana de tokens (2048 ctx) ─────────────────────────
    # Distribución para evitar cortes prematuros:
    #   system_prompt  ~100 tokens
    #   rules_chunk    ~400 tokens
    #   question       ~150 tokens
    #   respuesta      ~1000 tokens (teórico)
    #                  ──────────
    #   total          ~1650 tokens (seguro bajo 2048)
    MAX_RULES_CHARS   = 1800  # ~400 tokens
    MAX_QUESTION_CHARS = 700   # ~150 tokens

    # ── Palabras clave por tema → nombre del archivo de reglas ─
    TOPIC_MAP = {
        "compras":    ["compr", "proveedor", "orden de compra", "factur", "recepcion",
                       "reposicion", "pedido", "nota de pedido", "cotiz", "importac"],
        "ventas":     ["venta", "cliente", "factura", "nota de credito", "cobro",
                       "cuenta corriente", "comprobante", "cobranza"],
        "stock":      ["stock", "deposito", "inventario", "articulo", "mercaderia",
                       "transferencia", "movimiento", "merma", "scrap"],
        "auditoria":  ["auditoria", "trazabilidad", "user_id", "sod", "log", "seguridad",
                       "permiso", "rol", "riesgo", "fmeca"],
        "contable":   ["cuenta", "asiento", "iva", "iibb", "afip", "impuest", "ganancias",
                       "contabilidad", "libro", "citi", "sicore"],
    }

    # Mapa tema → archivo de reglas (nombre del .md en .brain/rules/)
    FILE_MAP = {
        "compras":  "compras_ventas_procesos.md",   # sección COMPRAS
        "ventas":   "compras_ventas_procesos.md",   # sección VENTAS
        "stock":    "system_logic_map.md",
        "auditoria":"audit_standard.md",
        "contable": "database_standards.md",
    }

    # ── System Prompt compacto (≈80 tokens) ──────────────────
    SYSTEM_PROMPT = (
        "Eres el asistente de Colosal ERP. "
        "Respondes en español, en pasos simples, SIN tecnicismos ni SQL. "
        "Usas los nombres del menú: 'Ve a COMPRA → Facturar...'. "
        "Si preguntam sobre auditoría técnica, respondés con precisión."
    )

    @classmethod
    def check_health(cls):
        """Verifica si Ollama está activo."""
        try:
            r = requests.get(cls.OLLAMA_TAGS_URL, timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    @classmethod
    def _detect_topic(cls, question: str) -> str:
        """
        Detecta el tema de la pregunta comparando con palabras clave.
        Retorna el nombre del tema o 'general'.
        """
        q = question.lower()
        scores = {}
        for topic, keywords in cls.TOPIC_MAP.items():
            scores[topic] = sum(1 for kw in keywords if kw in q)
        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else "general"

    @classmethod
    def _load_chunk(cls, topic: str, question: str) -> str:
        """
        Carga el chunk relevante del archivo de reglas correspondiente al tema.
        Si el archivo tiene secciones (##), extrae solo la sección que coincide.
        Limita a MAX_RULES_CHARS para no exceder el contexto.
        """
        filename = cls.FILE_MAP.get(topic)
        if not filename:
            # Fallback: leer el primer archivo disponible
            try:
                files = [f for f in os.listdir(cls.RULES_DIR) if f.endswith(".md")]
                filename = files[0] if files else None
            except Exception:
                return ""

        if not filename:
            return ""

        path = os.path.join(cls.RULES_DIR, filename)
        if not os.path.exists(path):
            return ""

        try:
            with open(path, "r", encoding="utf-8") as f:
                content = await f.read()
        except Exception as e:
            logger.error(f"Error leyendo reglas: {e}")
            return ""

        # Si el archivo tiene secciones ## MÓDULO X, extraer solo la relevante
        sections = re.split(r'\n## ', content)
        best_section = ""
        best_score = 0
        q = question.lower()

        for section in sections:
            score = sum(1 for kw in cls.TOPIC_MAP.get(topic, []) if kw in section.lower())
            if score > best_score:
                best_score = score
                best_section = section

        chunk = best_section if best_section else content
        return chunk[:cls.MAX_RULES_CHARS].strip()

    @classmethod
    def consult_rules(cls, question, user_context=None, history=None):
        """
        Consulta al LLM con prompt paginado optimizado para 2048 tokens.
        Soporta historial para poder 'continuar' respuestas cortadas.
        """
        if not cls.check_health():
            return {"error": "Ollama no responde. Iniciá el servicio desde la bandeja del sistema."}

        # 1. Detectar tema
        topic = cls._detect_topic(question)
        
        # 2. Cargar chunk de reglas
        rules_chunk = cls._load_chunk(topic, question)

        # 3. Construir historial compacto (últimos 3 mensajes para no saturar ctx)
        history_str = ""
        if history:
            for h in history[-3:]:
                role = "Asistente" if h.get("role") == "ai" else "Usuario"
                content = h.get("content", "")[:300]
                history_str += f"{role}: {content}\n"

        # 4. Construir prompt final
        full_prompt = (
            f"Contexto del sistema ({topic}):\n{rules_chunk}\n\n"
            f"{history_str}"
            f"Usuario: {question[:cls.MAX_QUESTION_CHARS]}\n\n"
            f"Asistente:"
        )

        payload = {
            "model": cls.DEFAULT_MODEL,
            "prompt": full_prompt,
            "system": cls.SYSTEM_PROMPT,
            "stream": False,
            "options": {
                # ── Calibrado para i5-8265U / 512 tokens ──
                "temperature":    0.3,
                "num_ctx":        2048,  # Aumentado para permitir respuestas largas
                "num_predict":    512,   # Aumentado de 150 a 512 (paginación interna)
                "num_thread":     4,     # 4 núcleos físicos del i5
                "repeat_penalty": 1.1,
                "stop": ["\n\n\n", "Pregunta:", "Question:", "Context:"]
            }
        }

        try:
            response = requests.post(cls.OLLAMA_URL, json=payload, timeout=300)  # 5 min timeout CPU
            if response.status_code == 200:
                answer = response.json().get("response", "").strip()
                return {
                    "response": answer,
                    "topic_detected": topic,       # Útil para depuración
                    "chunk_length": len(rules_chunk)
                }
            else:
                return {"error": f"Ollama Status {response.status_code}"}
        except Exception as e:
            return {"error": f"Falla LLM: {str(e)}"}

    @classmethod
    def audit_code_snippet(cls, module_name, code_snippet):
        """Revisión de código contra estándar de trazabilidad."""
        prompt = (
            f"Analiza el módulo '{module_name}'. "
            "¿Tiene user_id, created_at, user_id_update, updated_at en cada INSERT/UPDATE? "
            "Indicá qué falta."
        )
        return cls.consult_rules(prompt, user_context=f"CODE: {code_snippet[:200]}")

    @classmethod
    def gatekeeper_check(cls, change_description, code_diff=None):
        """Evaluación de riesgos antes de aplicar cambios críticos."""
        prompt = (
            f"Evalúa este cambio: {change_description[:200]}. "
            "¿Viola SoD, trazabilidad o borrado lógico? "
            "Decí BLOQUEAR o AJUSTAR con motivo breve."
        )
        return cls.consult_rules(prompt)
