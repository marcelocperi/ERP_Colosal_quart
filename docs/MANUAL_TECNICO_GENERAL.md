# 🛠️ Manual Técnico General del Sistema (MultiMCP)

## 1. Infraestructura y Arquitectura de Bajo Nivel

Esta sección describe cómo está construido el sistema "por debajo del capó", destinado a administradores de sistemas, devops y arquitectos.

### 1.1 Stack Tecnológico
*   **Lenguaje:** Python 3.9+ 
    *   Ejecución dinámica, tipado fuerte.
*   **Web Framework:** Flask (Microframework)
    *   Modelo WSGI estándar.
    *   Arquitectura modular basada en `Blueprints`.
*   **Application Server:** 
    *   **Producción:** Gunicorn (Green Unicorn) como servidor WSGI HTTP.
    *   **Desarrollo:** Werkzeug (servidor integrado de Flask).
*   **Base de Datos:** MariaDB 10.x / MySQL 8.0
    *   Motor transaccional InnoDB/XtraDB.
    *   Soporte JSON nativo para campos de metadatos flexibles.
*   **Frontend:** 
    *   Renderizado Server-Side (SSR) con **Jinja2**.
    *   CSS Vanilla (sin preprocesadores complejos) para máxima compatibilidad.
    *   JavaScript nativo (ES6+) para interactividad.

### 1.2 Arquitectura de Datos y Multi-Tenancy
El sistema utiliza un enfoque de **Aislamiento Lógico** (Logical Isolation) en una base de datos compartida.

*   **Columna Discriminadora:** Todas las tablas transaccionales y maestras incluyen `enterprise_id`.
*   **Pool de Conexiones Custom (`database.py`):**
    *   Implementa un pool de conexiones persistentes para reducir la latencia de handshake TCP.
    *   Maneja reconexiones automáticas (`ping=True`).
    *   Provee un *Context Manager* (`get_db_cursor`) que asegura el cierre de cursores y el commit/rollback transaccional.

### 1.3 Seguridad y Gestión de Sesiones
El sistema implementa un modelo de seguridad robusto y no convencional para soportar múltiples instancias por usuario.

*   **Multi-Tab Support (SID):** 
    *   Flask `session` estándar usa cookies firmadas. Este sistema extiende eso almacenando un diccionario `session['s']` dentro de la cookie.
    *   Cada pestaña del navegador genera un `sid` (Session ID) único.
    *   Esto permite que un usuario sea "Admin" en la Empresa A (Pestaña 1) y "Vendedor" en la Empresa B (Pestaña 2) simultáneamente.
*   **Protección CSRF:** TOKEN único generado por sesión, validado en cada request `POST`.
*   **Control de Accesos (RBAC + SoD):**
    *   Cache de permisos en memoria (TTL 300s) para evitar consultas repetitivas a la BD.
    *   Verificación de permisos mediante decoradores en rutas.

### 1.4 Diagrama de Infraestructura

```mermaid
graph TD
    Client[Navegador Cliente] -->|HTTPS| Proxy[Reverse Proxy (Nginx/Apache)]
    Proxy -->|Localhost:5005| AppServer[Gunicorn WSGI]
    
    subgraph "Application Core"
        AppServer -->|Request| Flask[Flask App]
        Flask -->|Blueprints| Modules[Módulos / Rutas]
        Modules -->|Context Manager| Pool[DB Connection Pool]
    end
    
    Pool -->|TCP/IP| DB[(MariaDB Database)]
    Flask -->|Async| Email[Servicio SMTP]
    Flask -.->|Read/Write| FS[File System (PDFs/Static)]
```

---

## 2. Descripción de Módulos (Uno a Uno)

El sistema está dividido en módulos funcionales independientes (Blueprints) que interactúan entre sí.

### 2.1 Core & Autenticación (`core`)
*   **Función:** Corazón del sistema. Maneja lo que sucede antes de entrar a una empresa.
*   **Responsabilidades:**
    *   Login / Logout / Recuperación de contraseña.
    *   Selección de Empresa (Tenant).
    *   Gestión de Usuarios Globales (`sys_users`).
    *   Auditoría de seguridad básica.

### 2.2 Administración de Empresas (`ent_bp`)
*   **Función:** Panel de control para configurar la instancia de la empresa.
*   **Responsabilidades:**
    *   Configuración fiscal (CUIT, IIBB, Inicio de Actividades).
    *   Personalización (Logos, Colores).
    *   Inicialización de datos maestros (`enterprise_init.py`).

### 2.3 Stock e Inventario (`stock`)
*   **Función:** Gestión física de los bienes.
*   **Componentes:**
    *   **Catálogo de Artículos:** Maestro de productos con soporte de metadatos dinámicos (JSON).
    *   **Movimientos:** Registro de entradas y salidas (ajustes, transferencias).
    *   **Depósitos:** Gestión multimacén.
    *   **Auditoría:** Procesos de conteo físico.

### 2.4 Compras (`compras`)
*   **Función:** Ciclo de aprovisionamiento (Gastos).
*   **Flujo Típico:** 
    1.  **Solicitud:** Pedido interno.
    2.  **Orden de Compra (OC):** Documento formal al proveedor.
    3.  **Recepción:** Ingreso físico (impacta Stock).
    4.  **Factura de Compra:** Registro fiscal (impacta Cuentas por Pagar).

### 2.5 Ventas (`ventas`)
*   **Función:** Ciclo comercial (Ingresos).
*   **Flujo Típico:**
    1.  **Presupuesto:** Cotización al cliente.
    2.  **Pedido de Venta:** Compromiso de entrega.
    3.  **Remito:** Documento de entrega (baja Stock).
    4.  **Factura de Venta:** Documento fiscal (genera deuda del cliente).

### 2.6 Tesorería / Fondos (`fondos`)
*   **Función:** Gestión financiera (Cash Flow).
*   **Responsabilidades:**
    *   **Cobranzas:** Recibos por facturas de venta.
    *   **Pagos:** Órdenes de pago a proveedores.
    *   **Cajas y Bancos:** Movimientos internos de dinero.
    *   **Valores:** Gestión de cheques de terceros y propios.

### 2.7 Contabilidad (`contabilidad`)
*   **Función:** Registro formal y legal de las operaciones.
*   **Características:**
    *   **Plan de Cuentas:** Árbol jerárquico configurable.
    *   **Libro Diario:** Asientos manuales y automáticos.
    *   **Motor de Asientos:** Los módulos operativos generan asientos automáticamente (ej: una Venta genera un asiento `Deudores a Ventas + IVA`).

### 2.8 Servicios Transversales (`services`)
Lógica de negocio encapsulada y compartida por todos los módulos:
*   **SoD Service:** Motor de creación de roles y validación de segregación de funciones.
*   **Georef Service:** API para normalización de direcciones y geolocalización.
*   **Email Service:** Cola de envío de correos asíncronos.
*   **Report Service:** Generador de PDFs y Excel.
*   **Biblioteca:** (Módulo Legacy) Gestión de préstamos y catálogo de libros físicos.

---
**Documento Generado:** 15/02/2026
**Departamento de Arquitectura de Sistemas**
