# Estándar de Auditoría Transaccional (CISA/SOX) - Colosal ERP

## 1. Requerimiento de Esquema (Base de Datos)
Todas las tablas de la base de datos (tanto operativas como de sistema) DEBEN contener los siguientes campos de trazabilidad:
- `user_id`: INT. ID del usuario que creó el registro.
- `created_at`: TIMESTAMP. Fecha de creación automática.
- `user_id_update`: INT. ID del usuario que realizó la última modificación.
- `updated_at`: TIMESTAMP. Fecha de última actualización (auto-update).

## 2. Reglas de Programación (Backend)
- Todo `INSERT` debe incluir el ID del usuario activo (`g.user['id']`).
- Todo `UPDATE` debe actualizar el campo `user_id_update` con el usuario de la sesión actual.
- No se permiten eliminaciones físicas (`DELETE`) en tablas transaccionales. Se debe usar borrado lógico (`activo = 0`).

## 3. Validación de Bloqueo
Si un módulo no cumple con el 100% de la existencia de estos campos, el sistema debe BLOQUEAR cualquier intento de asignación de roles o permisos a dicho módulo hasta que sea remediado.
