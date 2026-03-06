import traceback
from database import get_db_cursor

recreate_sql = [
    "SET FOREIGN_KEY_CHECKS = 0;",
    "DROP TABLE IF EXISTS `usuarios`;",
    """
    CREATE TABLE `usuarios` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `nombre` varchar(100) DEFAULT NULL,
      `apellido` varchar(100) DEFAULT NULL,
      `telefono` varchar(20) DEFAULT NULL,
      `email` varchar(100) DEFAULT NULL,
      `enterprise_id` int(11) NOT NULL,
      `created_by` int(11) DEFAULT NULL COMMENT 'ID Usuario Creador (Audit)',
      `updated_by` int(11) DEFAULT NULL COMMENT 'ID Usuario Modificador (Audit)',
      `created_at` timestamp NULL DEFAULT current_timestamp() COMMENT 'Fecha Creación (Audit)',
      `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp() COMMENT 'Fecha Modificación (Audit)',
      PRIMARY KEY (`id`,`enterprise_id`),
      UNIQUE KEY `unique_email_per_enterprise` (`email`,`enterprise_id`),
      KEY `idx_usuarios_ent_created_by` (`enterprise_id`,`created_by`),
      KEY `idx_usuarios_ent_email` (`enterprise_id`,`email`),
      CONSTRAINT `fk_usuarios_ent` FOREIGN KEY (`enterprise_id`) REFERENCES `sys_enterprises` (`id`) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """,
    "SET FOREIGN_KEY_CHECKS = 1;"
]

try:
    with get_db_cursor() as cursor:
        for sql in recreate_sql:
            cursor.execute(sql)
    print("✅ Tabla 'usuarios' recreada exitosamente con utf8mb4 y auditoría normalizada.")
except Exception as e:
    print(f"❌ Error al recrear tabla: {e}")
    traceback.print_exc()
