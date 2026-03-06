# Plan Maestro de Costos Industriales, Sourcing & R&D (MSAC v4.1)

Estimado usuario, este plan maestro se ha refactorizado y expandido profundamente para modelar una arquitectura industrial de escala real. Ahora cubre no solo la compra, venta y el ensamble de artículos, sino la **Ingeniería de Desarrollo de Productos (R&D)**, el control de **Calidad y Ensayos**, y el aseguramiento del marco **Técnico-Legal** necesario para comercializar (Bromatología, Normas ISO, Certificaciones, NDA de Fazón).

---

## 1. Roadmap de Implementación Industrial, Sourcing & Quality

| Fase / Sub-fase | Proceso Específico | Criticidad | Estado | Impacto Operativo |
| :--- | :--- | :--- | :--- | :--- |
| **Fase 1: Estructura Industrial, BOM & Estándares** | **Ingeniería Base** | **CRÍTICA** | **COMPLETADA** | |
| 1.1 Maestro Multi-Origen | Segmentación: Compra Local, Importada, Producción Propia y Fazón. | Media | **COMPLETADA** | Determina si el costo viene de factura o de orden de producción. |
| 1.2 Bill of Materials (BOM) | Definición de "Recetas" recursivas para artículos producidos o armados. | Alta | **COMPLETADA** | Soporta explosión multi-nivel (E.g. PC -> Mother -> Chips). |
| 1.3 Carga de Costos Indirectos | Overhead (Energía, Amortizaciones, Mano de Obra*, Fletes). | Crítica | **COMPLETADA** | Suma overhead al costo. *La Mano de Obra se imputa vía Standard Costing al no usar Payroll nativo.* |
| 1.4 RFQ Enrichment (Campañas) | Explosión de BOM y sugerencia automática masiva de proveedores a cotizar. | Media | **COMPLETADA** | Para fabricar 1000u, el sistema pide precios de los 5 insumos necesarios automáticamente. |
| 1.5 Módulo de Consignación & Fazón | Control de stock propio en talleres externos, o terceros en poder propio. | Alta | **COMPLETADA** | Liquidaciones automáticas de Fazón basadas en reportes de consumo del tercero. |
| 1.6 Calidad & Repositorio Técnico Legal | Integración de Ensayos/Calidad al costo. Repositorio de Bromatología/Protocolos. | Alta | **COMPLETADA** | Centraliza RNE/RNPA, Contratos SCM, Planos ISO y Fichas Técnicas. |
| 1.7 Audit. e Integridad de Código | Reconciliación Global v5, Auditoría de permisos CISA/SoD y remediación de UI. | Crítica | **COMPLETADA** | Garantiza que el código refleje el Plan de Cuentas e Industria. |
| 1.8 Módulo Producción Nativo | Consolidación de I+D, Costos, Documentación y Roles en nueva categoría. | Alta | **COMPLETADA** | Estructura organizada y segura bajo el rol "PRODUCCION". |
| 1.9 Reconciliador v5.1 & SoD Exceptions | Excepción de infracciones SoD para rol adminSys en desarrollo. Manual técnico generado en DOCX. | Media | **COMPLETADA** | Control de cumplimiento flexible sin bloquear ciclo de desarrollo. |

| **Fase 2: Proyectos R&D y Trazabilidad Transformacional** | **Capas & Vida del Producto** | **CRÍTICA** | **EN EJECUCIÓN** | |
| 2.1 Módulo Proyectos de Desarrollo | Gestión del ciclo de vida (Evaluación, I+D, Homologación Legal, Aprobado). | Alta | **COMPLETADA** | Permite registrar los gastos de I+D antes del lanzamiento del producto. |
| 2.2 Layer Roll-up (Producción) | Sistema capitaliza Costo = Σ(Costos de Materiales FIFO) + Overhead + Calidad. | Crítica | 10d | El costo de materiales consumidos se capitaliza en el producto terminado. |
| 2.3 Fazón Reception & Liquidación | Recepción de mercadería terminada por terceros, descontando partes consignadas. | Alta | 7d | Valúa el servicio del tercero de forma combinada con piezas entregadas previamente. |

| **Fase 3: Valuation, Margins & Finanzas** | **Motor de Rentabilidad Industrial** | **ALTA** | **PENDIENTE** | |
| 3.1 FIFO/WAC Across Layers | Salida de capas tanto para venta como para insumo de producción. | Crítica | 10d | Si sube un insumo importado, el sistema recalcula el costo del producto terminado en cascada. |
| 3.2 Dynamic Margin Engine | Determinación de precio de venta: Costo Industrial + Margen Objetivo + Impuestos. | Alta | 8d | Diferencia margen bruto vs margen neto industrial. |
| 3.3 Pricing Synchronization | Alertas al detectar variaciones en ensayos, importaciones, materias primas o sueldos. | Media | 5d | El PricingService reacciona a cambios en la última capa de producción o compras. |

| **Fase 4: Compliance, Audit & Scrap Control** | **Auditoría Normativa** | **ALTA** | **PENDIENTE** | |
| 4.1 Real vs Theoretical Loss | Comparativa de mermas/scrap reales vs teóricas del BOM. Diferencia el recupero. | Alta | 5d | Diferencia el Scrap vendible (Activo a VNR) del Scrap Destructivo (Cta de Pérdida). |
| 4.2 Alertas de Vencimientos Legales | Job automático que alerta expiración de RNE, RNPA, o ISO en Repositorio de Documentos. | Alta | 4d | Bloquea la comercialización o fabricación si el marco legal está caído. |
| 4.3 CISA/SOX Recon | Conciliación de cuentas de Balance e integridad de Roles SoD. | Crítica | **COMPLETADA** | Integridad contable y administrativa blindada. |

---

## 2. Definiciones Funcionales (Refactoring Industrial)

### A. Costeo Estándar (Payroll, Calidad y Certificaciones)
Dado que el ERP está centralizado en Operaciones y Finanzas y **no incluye un módulo de Liquidación de Haberes (Payroll) detallado**, Colosal aborda la nómina fabril y gastos de control de calidad usando un modelo de **Standard Costing**:
*   **Mano de Obra Directa (MOD):** Se establece un "Valor Hora Estándar" o "Costo Estándar por Batch" en la tabla `cmp_articulos_costos_indirectos`. Al cerrar una orden productiva, el sistema capitaliza este monto en el inventario final imputándolo contra la cuenta contable "5.3 Mano de Obra Directa".
*   **Ajustes Reales en Finanzas:** Cuando Finanzas carga la factura real del Laboratorio Catorce y medio, o la nómina salarial real enviada por el estudio contable, cualquier diferencia entre lo que la OP absorbió (Costo Estándar) y la factura real se envía a las cuentas de ajuste o resultados.

### B. El Repositorio Documental Polimórfico (Técnico/Legal)
Cualquier producto industrial propio o fabricado a Fazón requiere de un respaldo técnico, bromatológico (ANMAT/INAL), de seguridad (MSDS), o legal.
Para eso estructuramos **`sys_documentos_adjuntos`**:
*   Es **polimórfico**: Se asocia un PDF/Doc tanto a un Artículo (*Ficha Técnica*), a un Proveedor (*NDA de Fazón*), o a un Control de Calidad (*Protocolos*).
*   Cuenta con **Vencimientos Activos**: Generará alertas preventivas (EJ: *El Certificado RNE de las tapas de conservas vence en 30 días, no se podrá emitir producción*).

### C. Módulo de Proyectos de Desarrollo de Producto
Ningún producto de formulación compleja nace de la noche a la mañana. El nuevo módulo permite estructurar:
*   **`prd_proyectos_desarrollo`**: El maestro del proyecto donde confluyen las facturas de I+D (Ingeniería y Desarrollo), los prototipos, los ensayos, los permisos de las autoridades, el control de las distintas iteraciones de la BOM.
*   **Fases de Vida:** Evaluación -> I+D -> Homologación Legal -> Aprobado (Listo para producir).

### D. Consignación, Liquidaciones y Fazón
*   El material entregado al Taller Externo (Fazón) se traslada por sistema hacia un `stk_deposito` de propiedad tipo `FAZON_TERCERO`.
*   El Taller no "factura" sueldo; él consumió nuestros insumos y facturará **su Servicio de Costura/Armado**.
*   El sistema mediante `stk_liquidaciones_consignacion` recibe el reporte de mercaderías armadas, debita nuestra tela del depósito de Fazón, autoriza el ingreso de las camisas hechas, y nos incita a cargar la factura de servicio de tercero.

### E. Estructura del Plan de Cuentas (FACPCE)
El sistema ahora soporta de forma nativa la trazabilidad contable de los procesos industriales, importaciones y resultados extraordinarios mediante la siembra de la estructura FACPCE:
*   **Activos de Importación:** `1.4.05 Mercaderías en Tránsito (FOB + Flete + Seguro)`.
*   **Activos de Scrap / Recupero:** `1.6.01 Scrap con Valor de Recupero (Chatarra / Retazos)`, activo valorizado a su Valor Neto de Realización (VNR).
*   **Mano de Obra y Overhead:** Las cuentas de costeo industrial, ej. `5.3 Mano de Obra Directa`, `5.4.01 Energía`, `5.4.05 Control de Calidad`, `5.4.06 Ensayos Técnicos e I+D`.
*   **Importaciones y Gastos de Arribo:** Cuentas capitalizables como `5.7.01 Derechos de Importación` y `5.7.05 Honorarios Despachante`.
*   **Resultados y Contingencias (No capitalizables):** `6.4.01 Demurrage`, `6.4.03 Multas Aduaneras`, `7.1.01 Scrap sin Valor Residual`. Diferencia de cambio en cuenta financiera.

---

## 3. Estimación y Esfuerzo Consolidado (v4.1)
*Bases estructurales de base de datos terminadas para las fases transversales.*

| Hito Funcional / Módulo | Estado | Impacto General |
| :--- | :--- | :--- |
| **BOM & Costing Structure** | ✅ FINALIZADO | Define ingeniera, "overhead" estándar. |
| **Plan de Cuentas Industrial/Import/Scrap** | ✅ FINALIZADO | Estructura FACPCE completa sembrada. |
| **Repositorio Documental / Legal** | ✅ FINALIZADO | Soporte Bromatólogico, ISO, Contratos, QA. |
| **Proyectos de I+D** | ✅ FINALIZADO | Cuna del producto antes de llegar a la BOM. |
| **Consignaciones y Fazón** | ✅ FINALIZADO | Control de Taller, Liquidaciones de Consumos. |
| **Production FIFO Capas & Rentabilidad** | ⏳ EN EJECUCIÓN | Integridad Financiera a nivel auditoría internacional. |

> [!IMPORTANT]
> **Plan de Acción Inmediato**: Has validado que la arquitectura industrial (Repositorio Legal, Estándares de Costos para RRHH, Calidad, Fazón) quede completamente asimilada al plan original. El código de las rutas y plantillas asociadas (HTML) a estos flujos es el siguiente paso lógico.
