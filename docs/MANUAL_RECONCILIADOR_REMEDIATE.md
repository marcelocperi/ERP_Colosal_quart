# Manual Técnico: Reconciliador & Remediador Global (v5.1)

## 1. Introducción
El script `python_check_remediate.py` es la herramienta central de integridad de código y cumplimiento normativo (CISA/SOX) del sistema Colosal ERP. Su función es garantizar la consistencia entre la definición del menú, los controladores de rutas en Python, los archivos de plantillas HTML y los permisos de seguridad en la base de datos.

## 2. Funcionalidades Principales

### 2.1 Reconciliación de Rutas e Interfaz
- **Alineación Menú-Código:** Escanea `menu_structure.json` y verifica que cada entrada tenga un handler `(.route)` correspondiente en los archivos de rutas de Python.
- **Remediación de Plantillas:** Si una ruta está definida pero su archivo `.html` asociado no existe, el script genera automáticamente un template base para evitar errores 500.

### 2.2 Auditoría de Permisos (CISA Compliance)
- **Extracción de Decoradores:** Escanea el código fuente buscando el decorador `@permission_required('permiso')`.
- **Integridad de Base de Datos:** Compara los permisos detectados en el código con la tabla `sys_permissions`.
- **Auto-Siembra (Auto-Seed):** Crea automáticamente cualquier permiso faltante en la base de datos para el `enterprise_id = 0` (Entorno de Desarrollo/Sistema).

### 2.3 Segregación de Funciones (SoD - Segregation of Duties)
- **Detección de Conflictos:** Analiza la tabla `sys_role_permissions` buscando roles que posean permisos incompatibles según las mejores prácticas de control interno.
- **Conflictos Definidos:**
    - `compras.admin` vs `ventas.admin`
    - `contabilidad.admin` vs `fondos.admin`
    - `cost_accounting` (Auditoría) vs `view_precios` (Operación)
- **Gestión de Excepciones:** Permite excluir roles específicos de la alerta de conflictos para facilitar el desarrollo (ej. el rol `adminSys` está excluido de las alertas SoD).

## 3. Log de Incidentes y Auditoría
Cualquier discrepancia o remediación realizada se inscribe automáticamente en la tabla `sys_transaction_logs` con los siguientes atributos:
- **Módulo:** `RECONCILIADOR`
- **Categoría:** `INTEGRIDAD / SEGURIDAD`
- **Severidad:** `4 (Crítica)`
- **Detalle:** Un desglose línea por línea de cada hallazgo (ej. Permisos creados, templates generados, conflictos SoD detectados).

## 4. Historial de Cambios Recientes (v5.1)
- **Integración Industrial:** Soporte para el nuevo módulo de `PRODUCCION` y sus permisos asociados.
- **Manejo de Enterprise ID:** Sincronización corregida para operar sobre el ID 0 en entornos locales.
- **Exclusiones de Desarrollo:** Implementación de clausulas `continue` para roles de super-usuario en auditorías SoD.

---
**Desarrollado por:** Antigravity AI
**Estado:** Activo / Producción
**Última Revisión:** 2026-03-02
