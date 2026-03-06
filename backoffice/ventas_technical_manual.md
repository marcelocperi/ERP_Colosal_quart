# Manual Técnico: Módulo de Ventas y Facturación (MSAC v4.0)

## 1. Arquitectura de Datos (Core)

El módulo de ventas de Colosal utiliza una arquitectura centralizada en la tabla `erp_comprobantes`, diferenciando las operaciones por el campo `tipo_operacion = 'VENTA'`.

### Tablas Críticas
| Tabla | Descripción | Propósito |
|-------|-------------|-----------|
| `erp_terceros` | Maestro de Clientes | Almacena datos con `es_cliente = 1`. |
| `erp_comprobantes` | Cabecera de Documentos | Facturas, NC, ND, Remitos. Posee campos fiscales (CAE, Vto). |
| `erp_comprobantes_detalle`| Líneas de Venta | Detalles de artículos, cantidades y alícuotas de IVA. |
| `erp_comprobantes_impuestos`| Percepciones IIBB | Detalle dinámico de percepciones por jurisdicción. |
| `fin_factura_cobros` | Cobranzas Inmediatas | Registra el medio de pago usado al facturar. |
| `erp_terceros_cm05` | Coeficientes CM05 | Coeficientes para Convenio Multilateral (IIBB). |

---

## 2. Lógica de Facturación Automática

El proceso de facturación (`procesar_factura`) realiza las siguientes acciones atómicas:

1.  **Validación de Numeración**: Obtiene el próximo número legal vía `NumerationService`.
2.  **Cálculo Fiscal Dinámico**:
    *   Base Imponible (Neto) + IVA.
    *   **Percepciones IIBB**: Se cruzan las jurisdicciones donde el Cliente es sujeto (`erp_datos_fiscales`) con aquellas donde la Empresa es Agente (`sys_enterprises_fiscal`).
3.  **Afectación de Stock**:
    *   Ventas: Descuenta del depósito origen (`signo_stock = -1`).
    *   Notas de Crédito: Reingresa mercadería (`signo_stock = 1`).
4.  **Integración Contable**: Genera un asiento automático (`_generar_asiento_contable`) impactando en Ventas, IVA Débito, Deudores por Ventas y Recaudaciones a Depositar.
5.  **AFIP (CAE)**: Solicita autorización electrónica mediante `AfipService`.

---

## 3. Cuenta Corriente (Balance de Cliente)

El saldo de un cliente se calcula en tiempo real mediante la siguiente fórmula:

```sql
Saldo = Sum(Facturas + ND) - Sum(Recibos + NC)
```

*   **Débitos**: Facturas (Tipo 001, 006, 011), Notas de Débito (005, 010, 015).
*   **Créditos**: Recibos (REC) y Notas de Crédito (003, 008, 013).

---

## 4. Integración de IA (Ollama)

El modelo **Colosal-Ventas** está entrenado para responder sobre:
1.  **Reglas de Facturación**: Qué letra de comprobante corresponde (A, B, C) según `tipo_responsable`.
2.  **Audit de Percepciones**: Explicar por qué se aplicó una percepción de IIBB en una jurisdicción específica.
3.  **Análisis de Crédito**: Consultar estados de aprobación de condiciones de pago.

---

## 5. Consultor Standalone (CLI)

El utilitario `utils/colosal_ventas_consultant.py` permite:
*   Ver el Top 10 Clientes por facturación acumulada.
*   Consultar el Estado de Deuda de un Cliente por CUIT.
*   Listar comprobantes pendientes de CAE.
