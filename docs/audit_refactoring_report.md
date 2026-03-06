# Reporte de Auditoría y Refactorización del Módulo de Ventas y Contabilidad

Basado en la auditoría inicial de la base de datos y la recolección de deficiencias críticas para la integridad contable y legal, se han implementado las siguientes modificaciones y refactorizaciones en la capa de datos y en la lógica comercial que afecta al sistema de Ventas (`ventas/routes.py`).

## 1. Modificaciones de Esquema (Schema Refactoring)
Se ejecutó el script `update_schema_audit.py` añadiendo reglas de integridad y trazabilidad faltantes sin destruir datos previos:
- **`erp_comprobantes_impuestos`**: 
  - Se agregó `jurisdiccion_id` (INT) e `impuesto_id` (INT). De esa forma se evitan los registros en formato texto que rompían la consistencia.
  - Se añadió `user_id` y `created_at` para registrar la persona que generó la aplicación de la retención/percepción y cuándo lo hizo.
- **`fin_factura_cobros`**: 
  - CRÍTICO: Se agregó la columna `cuenta_contable_snapshot_id`. Esto asegura que si una cuenta en `fin_medios_pago` se modifica en el futuro, el histórico financiero permanece inmutable según los principios de contabilidad generalmente aceptados.
- **`stk_existencias`**: 
  - Añadido el campo `ubicacion` para llevar gestión fina (ej: Pasillo 2, Estante B).

## 2. Refactorización de Código (`multiMCP/ventas/routes.py`)

### A. Trazabilidad de Percepciones e Impuestos
En `procesar_factura`:
- Se han reemplazado las agrupaciones puramente por "provincia en texto" para utilizar IDs formales de Jurisdicciones y obtener el `impuesto_id` con nombre tipo "Ingresos Brutos" desde la tabla de los maestros de impuestos.
- Ahora, cada percepción calculada dinámicamente que se inserta en `erp_comprobantes_impuestos` registra adecuadamente el responsable (`user_id`) y el `impuesto_id`. 

*(Nota Técnica: Las referencias legacy_columns como `importe_percepcion_arba` se han dejado temporalmente insertándose en `erp_comprobantes` para no romper retrocompatibilidad inmediata de lectura de comprobantes con otras pantallas (como reportes fiscales). Sus remociones definitivas requerirán una revisión de toda la app `contabilidad`.)*

### B. Inmutabilidad de Pagos
- Al registrar el pago (`fin_factura_cobros`), ahora se realiza una consulta **en tiempo de cobro** a la tabla `fin_medios_pago` para registrar su `cuenta_contable_id` en `cuenta_contable_snapshot_id`.

### C. Trazabilidad de Stock Correctiva
- Antes, los movimientos de stock marcaban "destino nulo" en todos los casos incluyendo anulaciones (NC), perdiendo la trazabilidad de entrada/salida.
- Ahora, si el comprobante es de venta normal `deposito_origen_id` se llena e invierte si es una Nota de Crédito asignándole explícitamente el `deposito_destino_id`.

### D. Asientos Contables Perfectos
- La función `_generar_asiento_contable` ahora recibe el `$user_id` en contexto.
- Se implementó la numeración local por empresa (`numero_asiento`) que faltaba, incrementándola lógicamente según el máximo por grupo de *enterprise*.
- Se rellenan explícitamente `user_id`, lo que permite un control transparente desde libros mayores y de gestión sobre qué usuario afectó un asiento.
- Error Control: Ahora, en caso de fallar o no encontrar las cuentas base correspondientes (como '1.3.01', etc.), hace explicit warning sobre las dependencias faltantes para una pronta regularización maestra en lugar de fallar silenciosamente.

### E. Integración de Vencimientos
- Si la factura cuenta con `condicion_pago_id`, automáticamente procesa la resolución de fechas de vencimiento considerando la fecha de la factura + X días pre-pactados, y los guarda integralmente en `fecha_vencimiento`.

## 3. Saneamiento de Datos Maestros
- Se ejecutó un script de regularización sobre la tabla `erp_terceros`.
- Se resolvieron inconsistencias comunes como valores por defecto nulos en `cuit` (ahora rellenado con '00000000000' cuando viene vacío), `tipo_responsable` ('Consumidor Final'), `condicion_pago_id` (1 = Contado) y `email`. Esto elimina los riesgos de caídas de aplicación por dereferencias nulas durante el bucle de facturación.

## 4. Refactorización del Módulo de Compras (Homologación)
Para mantener la consistencia bidireccional, se ampliaron las mismas rigorías de inmutabilidad aplicadas a Ventas hacia Compras (`multiMCP/compras/routes.py`):
- **Registro de Comprobantes de Compra:** Ahora evalúa automáticamente la `fecha_vencimiento` basado en la condición de pago del proveedor. Además implementa la misma inserción de trazabilidad forense hacia `erp_comprobantes_impuestos` (con ID de usuario y jurisdicción) para el IVA y percepciones provinciales (ARBA, AGIP) capturadas del recibo/factura externa.
- **Inmutabilidad en Pagos a Proveedores:** La función de procesamiento de pagos (`api_procesar_orden_pago`) fue modificada. Ahora, al grabar las referencias transaccionales en `fin_ordenes_pago_medios`, inspecciona temporalmente la tabla `fin_medios_pago` y hace un freeze snapshot del `cuenta_contable_snapshot_id`. Esto asegura la auditoría inmutable de cuentas en los ciclos de tesorería y caja chica.

## 5. Próximos pasos recomendados
Para finiquitar el cambio en su totalidad, se sugiere proceder sobre las siguientes áreas:

1. **Hardcodes Residuales:** Refactorizar el motor de vistas e impresiones fiscales para que consuma un 100% de `erp_comprobantes_impuestos` en un solo join relacional en vez de leer campos hardcodeados estáticos (ARBA, AGIP) de la cabecera.
2. **Asientos Automáticos de Compra:** Faltaría integrar o verificar la generación de asientos contables automáticos (Debe/Haber) para las compras y pagos, insertando apropiadamente en la tabla `cont_asientos` de la misma manera que en Ventas.
