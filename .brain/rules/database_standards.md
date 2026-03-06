# Estándar de Integridad de Datos y DB - Colosal ERP

## 1. Campos Obligatorios de Trazabilidad
Toda tabla del sistema debe poseer obligatoriamente:
1. `user_id`: Identifica al creador del registro.
2. `created_at`: Marca de tiempo de creación.
3. `user_id_update`: Identifica al último modificador.
4. `updated_at`: Marca de tiempo de actualización.

## 2. Tipos de Datos y Escalamiento
- IDs: Usar `INT` o `BIGINT`.
- Moneda: Usar `DECIMAL(19,4)` para evitar errores de redondeo.
- Fechas: Usar `DATETIME` para registros operativos y `DATE` para vencimientos.

## 3. Restricciones de Integridad
- No se permiten registros huérfanos. Se deben usar llaves foráneas (`FOREIGN KEY`) con nombres descriptivos (fk_tabla_origen_destino).
- Los índices deben ser creados para todos los campos de búsqueda frecuente (CUIT, Código, Email).

## 4. Auditoría de Esquema
El servicio `AuditCertificationService` analizará periódicamente que el 100% de las tablas cumplan con el Punto 1. Cualquier omisión descalifica al módulo para operar bajo normas SOX.
