-- ============================================================
--  sys_ai_feedback — Tabla de Feedback del CORE AUDITOR
--  Persiste pregunta + respuesta + rating (0-10) del usuario.
--
--  PROPÓSITO DEL RANKING:
--  ─────────────────────
--  1. REVISIÓN DE CALIDAD: ratings <= 3 quedan marcados como
--     "PÉSIMO/BAJO" para que el SaaS Owner revise el prompt
--     y mejore las respuestas del modelo.
--
--  2. ESTADÍSTICAS: /api/ai/stats expone avg_rating, histograma
--     y las peores respuestas → visible en panel de admin.
--
--  3. MEJORA CONTINUA: A futuro, las preguntas con baja calidad
--     pueden usarse para re-entrenamiento (fine-tuning) del modelo
--     local o para refinar las reglas en .brain/rules/*.md
--
--  4. AUDITORÍA SOX: Cada interacción queda trazada con user_id,
--     enterprise_id y timestamps → cumple norma de trazabilidad.
-- ============================================================
CREATE TABLE IF NOT EXISTS sys_ai_feedback (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- Trazabilidad CISA/SOX obligatoria
    enterprise_id INT NOT NULL DEFAULT 0,
    user_id INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id_update INT NULL,
    updated_at DATETIME NULL ON UPDATE CURRENT_TIMESTAMP,
    -- Contenido de la interacción
    question TEXT NOT NULL COMMENT 'Pregunta del usuario al LLM',
    response MEDIUMTEXT NULL COMMENT 'Respuesta del LLM',
    -- Rating del usuario
    rating TINYINT NULL COMMENT '0-10 asignado por el usuario',
    rating_label VARCHAR(20) NULL COMMENT 'PÉSIMO/BAJO/ACEPTABLE/BUENO/MUY BUENO/EXCELENTE',
    rated_at DATETIME NULL COMMENT 'Cuándo se calificó',
    -- Metadatos adicionales
    model_used VARCHAR(100) NULL DEFAULT 'tinyllama:latest',
    response_time_s DECIMAL(6, 2) NULL COMMENT 'Segundos que demoró el LLM',
    INDEX idx_feedback_ent (enterprise_id),
    INDEX idx_feedback_user (user_id),
    INDEX idx_feedback_rating (rating),
    INDEX idx_feedback_date (created_at),
    CONSTRAINT fk_aifeedback_user FOREIGN KEY (user_id) REFERENCES sys_users(id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'Historial de interacciones con CORE AUDITOR + ratings de calidad';
-- Vista útil para panel de admin: peores respuestas a revisar
CREATE OR REPLACE VIEW vw_ai_feedback_bad AS
SELECT f.id,
    e.nombre AS empresa,
    u.username AS usuario,
    f.question,
    LEFT(f.response, 300) AS response_preview,
    f.rating,
    f.rating_label,
    f.rated_at,
    f.created_at
FROM sys_ai_feedback f
    JOIN sys_users u ON f.user_id = u.id
    JOIN sys_enterprises e ON f.enterprise_id = e.id
WHERE f.rating <= 3
    AND f.rating IS NOT NULL
ORDER BY f.rated_at DESC;