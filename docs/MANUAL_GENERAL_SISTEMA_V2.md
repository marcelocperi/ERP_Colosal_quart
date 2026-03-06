# Manual General del Sistema MultiMCP
**Versión 2.0 - Documentación Integral**

Este manual describe la operatividad completa del sistema, abarcando infraestructura, gestión multi-empresa y módulos operativos. Diseñado para usuarios clave y auditores.

---

## 1. Infraestructura y Arquitectura
El sistema opera bajo un modelo **Multi-Tenant** (Múltiples empresas en una sola instalación), garantizando el aislamiento de datos y la seguridad mediante el uso estricto del `enterprise_id` en todas las consultas y registros.

## 2. Gestión de Empresas
Módulo administrativo para la creación y configuración de nuevos entornos de trabajo. Permite definir la configuración fiscal, puntos de venta (numeración), y parámetros operativos de cada empresa.

## 3. Gestión de Stock e Inventario
Control total de los bienes de cambio, desde su definición hasta su movimiento físico. 
Se contemplan múltiples depósitos por empresa, trazabilidad de artículos, gestión de lotes y control de stock mínimo. El sistema actualiza automáticamente los saldos tras cada iteración en Ventas y Compras.

## 4. Módulo de Compras e Importaciones
Ciclo completo de aprovisionamiento con controles de segregación de funciones.

### 4.1 Ciclo de Compras Locales
- **Órdenes de Compra:** Especificación de condiciones, artículos y aprobación de gerencias (Tesorería/Compras).
    - **Identificación de Proveedores:** El sistema permite la carga y recuperación de datos de proveedores mediante el **Código Interno** (ej. SUP-0045), la **Razón Social** o el **CUIT**.
    - **Búsqueda Rápida:** La interfaz de "Nueva Orden" incluye un buscador optimizado por código de barras/interno para agilizar la carga manual.
- **Recepción de Mercadería:** Ingreso de stock y comprobación 3-Way Match (Orden -> Remito -> Factura).
- **Liquidación y Pago:** El circuito vincula la deuda en Cuenta Corriente para su posterior cancelación, manteniendo siempre el vínculo con el código interno del proveedor para reporte contable.

### 4.2 Gestión de Importaciones y Logística (Novedad V2.0)
El sistema ahora soporta un panel dedicado integral para importaciones:
- **Vessel Tracking y Despachos:** Permite cargar las Órdenes de Compra en despachos aduaneros, introduciendo variables logísticas como el seguimiento del buque (`vessel_mmsi`, `vessel_name`).
- **Costos Unitarios de Importación (CUI):** Cálculo exacto y transparente de los costos de nacionalización distribuidos por artículo. El desglose permite discriminar entre *Tributos Aduaneros* y *Gastos Operativos*.
- **Gestión de Demoras de Puerto (Demurrage):** Novedoso sistema de alertas automáticas. Al ingresar los *Días Libres de Puerto*, *Fecha de Arribo* y *Costo Diario de Demora*, un proceso automático (Cron) notifica vía email a logística sobre posibles incurrencias en costos extras antes del vencimiento.

## 5. Módulo de Ventas
Gestión comercial desde la cotización hasta la facturación.
Generación de facturas electrónicas, emisión de remitos con descargo automático de stock, control de cuentas corrientes de clientes, y automatización de percepciones según padrón fiscal.

## 6. Tesorería y Fondos
Administración centralizada del flujo de dinero (Cash Flow).

### 6.1 Órdenes de Pago y Cobranzas
- Permite saldar facturas de proveedores (Órdenes de Pago) y registrar ingresos de clientes (Recibos).
- Soporte para múltiples medios de pago: Cheques (propios y de terceros), Transferencias, E-checks, DEBIN y Efectivo.
- **Gestión de Retenciones:** Cálculo y aplicación automática de retenciones (Ingresos Brutos y Ganancias) según la condición fiscal del tercero al emitir Órdenes de Pago, emitiendo el correspondiente certificado.

---

## 7. Diagramas de Colaboración por Rol/Proceso

### 7.1 Flujo Operativo de Importaciones
*(El eje X indica los Pasos del Proceso; El eje Y indica el Rol Interviniente)*

| Rol Interviniente | 1. Emisión OC | 2. Seguimiento y Arribo | 3. Gastos y CUI | 4. Autorización Pago | 5. Cancelación / OP |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Comprador** | **[INICIO]** | | | | |
| **Op. Logístico** | | **[GESTIONA]** (Alerta Demora) | | | |
| **Despachante/Adm.** | | | **[CALCULA CUI]** | | |
| **Gerencia / CFO** | | | | **[AUTORIZA]** | |
| **Tesorería** | | | | | **[EMITE OP Y RETIENE]** |

### 7.2 Flujo Cuentas por Pagar (Compras Locales)
| Rol Interviniente | 1. Ingreso Factura | 2. Conformación | 3. Selección y Retención | 4. Emisión Orden Pago | 5. Contabilización |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Recepción/Admin.**| **[CARGA FACTURA]** | | | | |
| **Aprobador** | | **[VALIDA 3-WAY]** | | | |
| **Analista Tesorería**| | | **[APLICA RETENCION]** | **[PAGA (E-check/Transf)]** | |
| **Sistema** | | | | | **[ASIENTO AUTO]** |

---

## Anexo: Matriz de Control Interno (Auditoría)
Matriz de riesgos operativos y controles mitigantes implementados en el sistema.

| Riesgo | Control de Sistema | Mitigación |
| :--- | :--- | :--- |
| **Acceso no autorizado** | Autenticación Multi-Tenant por Sesión | Aislamiento lógico de empresas. |
| **Compras Ficticias** | Segregación Solicitante vs Aprobador | Roles SoD estrictos requeridos en la plataforma. |
| **Pagos Fraudulentos** | Validación 3-Way Match | Exige Orden de Compra y Recepción conformadas previas. |
| **Costos Desmedidos Puerto** | Alertas Cron de Demurrage | Notificación temprana asincrónica sobre vencimiento de Días Libres. |
| **Fuga de Stock** | Trazabilidad de Movimientos | Registro inmutable de usuario, fecha y lote exacto extraído. |
| **Error en Identificación** | Búsqueda por Código Interno / CUIT | Validación cruzada de datos del proveedor en tiempo real. |

---

## 8. Guía de Interfaz: Nueva Orden de Compra
La pantalla de creación de órdenes ha sido mejorada para garantizar la exactitud en la selección del proveedor.

![Nueva Interfaz de Orden de Compra](/C:/Users/marce/.gemini/antigravity/brain/203991db-88a7-4508-8b66-e32a30876953/nueva_orden_compra_mejorada_1772196785278.png)
*Visualización de la nueva interfaz con búsqueda rápida por código y panel de información.*

### Características destacadas:
1. **Búsqueda Rápida por Código:** Permite ingresar directamente el código interno (ej. SUP-0045). El sistema buscará coincidencias y seleccionará al proveedor automáticamente si el código es exacto.
2. **Panel de Validación:** Al seleccionar un proveedor, se despliega un cuadro informativo con su Razón Social, CUIT y Código Interno, asegurando la trazabilidad antes de confirmar la operación.
3. **Integración con CUIT:** La búsqueda por combo también soporta el ingreso de CUIT para aquellos proveedores que no posean código interno asignado.
