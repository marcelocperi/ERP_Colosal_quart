# Manual Técnico del Módulo de Compras (Colosal MSAC v4.0)

Este documento contiene la especificación técnica de alta fidelidad del módulo de Compras, incluyendo el motor de Sourcing Multi-Origen, Bill of Materials (BOM) recursivo y el sistema de Consignación Industrial.

## 1. Arquitectura de Base de Datos (Core Compras)
### 1.1 Maestro de Precios y Sourcing
*   `cmp_articulos_proveedores`: Repositorio central de costos por proveedor.
    *   `precio_referencia`: Base para el cálculo de costos comerciales.
    *   `es_habitual`: Flag para la selección automática en RFQs.
*   `cmp_sourcing_origenes`: Clasificación de origen (Local, Importado, Producción Propia, Fazón).

### 1.2 Estructura Industrial (BOM)
*   `cmp_recetas_bom`: Cabecera de la receta por producto.
*   `cmp_recetas_detalle`: Componentes, cantidades y % de merma esperada. Soporta recursividad (sub-ensambles).
*   `cmp_articulos_costos_indirectos`: Carga de Overheads (Mano de obra, Energía, etc.).

### 1.3 Consignación y Tenencia
*   `cmp_consignaciones`: Cabecera de flujos de stock en terceros.
*   `cmp_items_consignacion`: Detalle de items enviados/recibidos.
*   `cmp_liquidaciones_consignacion`: Registro de consumos (Avisos de Venta) y estado de facturación.

## 2. Lógica de Negocio (Algoritmos)
### 2.1 Cálculo de Costo Industrial (Roll-up)
El costo de un producto producido se determina mediante:
`Costo Total = Σ (Costo Componente * Cantidad * (1 + Merma)) + Σ (Gastos Indirectos)`

**Algoritmo Recursivo:**
1.  Si el componente tiene una receta activa, se llama a sí mismo para calcular el costo de producción.
2.  Si no tiene receta, busca el costo de sourcing (Última Recepción o Mejor Precio en Maestro).
3.  Calcula el precio sugerido aplicando el `porcentaje_margen_esperado`.

### 2.2 Sourcing Strategy
*   `BEST_PRICE`: Selecciona el proveedor con el menor `precio_referencia`.
*   `LAST_RECEPTION`: Busca la última factura de compra en `erp_comprobantes_detalle`.
*   `HABITUAL`: Selecciona al proveedor marcado por el comprador como preferente.

## 3. Integración AI (Colosal Ollama)
El sistema utiliza el modelo local para asistir en:
1.  **Explosión de RFQs**: Generación de solicitudes de cotización automáticas basadas en el BOM.
2.  **Auditoría CISA**: Detección de stock inmovilizado en consignación.
3.  **Análisis de Desvíos**: Comparación entre costo teórico (BOM) y costo real (Capas FIFO).
