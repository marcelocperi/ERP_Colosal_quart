# MATRIZ DE SEGREGACIÓN DE FUNCIONES (Separation of Duties)
# Basado en SOX, COSO y mejores prácticas de auditoría global

## Principios Fundamentales de SoD

**Regla de Oro**: Ninguna persona debe tener control completo sobre una transacción crítica desde el inicio hasta el final.

### Conflictos Críticos que DEBEN Evitarse:

1. **Compras**:
   - ❌ Quien SOLICITA no debe APROBAR
   - ❌ Quien APRUEBA no debe RECIBIR mercadería
   - ❌ Quien RECIBE no debe REGISTRAR facturas
   - ❌ Quien REGISTRA facturas no debe PAGAR

2. **Ventas**:
   - ❌ Quien VENDE no debe FACTURAR
   - ❌ Quien FACTURA no debe COBRAR
   - ❌ Quien COBRA no debe CUSTODIAR efectivo
   - ❌ Quien CUSTODIA no debe CONCILIAR

3. **Tesorería**:
   - ❌ Quien SOLICITA pagos no debe AUTORIZAR
   - ❌ Quien AUTORIZA no debe EJECUTAR
   - ❌ Quien EJECUTA no debe CONCILIAR

4. **Inventarios**:
   - ❌ Quien CUSTODIA no debe REGISTRAR
   - ❌ Quien REGISTRA no debe CONTAR físicamente
   - ❌ Quien CUENTA no debe AJUSTAR diferencias

---

## Roles Propuestos (13 roles funcionales)

### OPERACIONES

#### 1. **SOLICITANTE_COMPRAS**
- **Función**: Inicia solicitudes de compra por stock crítico
- **Puede**: Ver stock, crear solicitudes
- **NO puede**: Aprobar, recibir, pagar
- **Permisos**: `view_articulos`, `view_movimientos`

#### 2. **COMPRADOR**
- **Función**: Gestiona órdenes de compra y proveedores
- **Puede**: Crear/gestionar OC, administrar proveedores
- **NO puede**: Aprobar OC, recibir mercadería, facturar
- **Permisos**: `create_orden_compra`, `view_proveedores`, `view_compras`

#### 3. **APROBADOR_COMPRAS**
- **Función**: Autorización gerencial de compras
- **Puede**: Aprobar/rechazar OC
- **NO puede**: Crear OC, recibir, pagar
- **Permisos**: `aprobar_ordenes` (nuevo permiso necesario)

#### 4. **RECEPCIONISTA**
- **Función**: Recibe y registra entrada de mercadería
- **Puede**: Recibir stock, actualizar inventario
- **NO puede**: Crear OC, facturar, pagar
- **Permisos**: `receive_stock`, `view_articulos`

#### 5. **VENDEDOR**
- **Función**: Atención al cliente y pedidos
- **Puede**: Consultar stock, crear presupuestos/pedidos
- **NO puede**: Facturar, cobrar, dar descuentos >X%
- **Permisos**: `view_clientes`, `create_presupuesto`, `view_pedidos`, `view_articulos`

#### 6. **FACTURACION**
- **Función**: Emisión de comprobantes (compras y ventas)
- **Puede**: Facturar ventas, registrar facturas de compra
- **NO puede**: Vender, cobrar, pagar
- **Permisos**: `facturar_ventas`, `facturar_compras`, `view_compras`

#### 7. **ALMACENISTA**
- **Función**: Custodia y movimiento físico de stock
- **Puede**: Mover stock, crear transferencias, gestionar depósitos
- **NO puede**: Vender, registrar valuaciones, ajustar diferencias
- **Permisos**: `view_articulos`, `view_movimientos`, `create_transferencia`, `admin_depositos`, `admin_logistica`

---

### TESORERÍA Y FINANZAS

#### 8. **CUENTAS_POR_PAGAR**
- **Función**: Registro y control de pagos a proveedores
- **Puede**: Registrar facturas, crear órdenes de pago, conciliar
- **NO puede**: Aprobar pagos, ejecutar pagos
- **Permisos**: `create_pago`, `view_pagos`, `view_compras`, `conciliar_banco`

#### 9. **AUTORIZADOR_PAGOS**
- **Función**: Aprobación gerencial de desembolsos
- **Puede**: Aprobar/rechazar órdenes de pago
- **NO puede**: Crear órdenes, ejecutar pagos
- **Permisos**: `autorizar_pagos` (nuevo permiso necesario)

#### 10. **TESORERO**
- **Función**: Ejecución de pagos y manejo de cuentas bancarias
- **Puede**: Ejecutar pagos aprobados, administrar cuentas/medios
- **NO puede**: Aprobar pagos, conciliar
- **Permisos**: `ejecutar_pagos` (nuevo), `admin_cuentas`, `admin_medios_pago`

#### 11. **COBRANZAS**
- **Función**: Gestión de cobranzas y recibos
- **Puede**: Cobrar, emitir recibos, consultar estados de cuenta
- **NO puede**: Facturar, custodiar efectivo, ajustar saldos
- **Permisos**: `view_cobranzas`, `create_recibo`, `view_estados_cuenta`

---

### CONTROL Y SUPERVISIÓN

#### 12. **CONTADOR**
- **Función**: Registro contable y reportes fiscales
- **Puede**: Ver libros IVA, plan de cuentas, reportes contables
- **NO puede**: Ejecutar operaciones, aprobar, modificar maestros
- **Permisos**: `view_reportes`, `view_balance`, `admin_plan_cuentas`

#### 13. **AUDITOR**
- **Función**: Supervisión, trazabilidad y auditoría interna
- **Puede**: Ver TODO (read-only), inventarios físicos, logs
- **NO puede**: Modificar NADA (solo lectura)
- **Permisos**: `auditar_inventario`, `view_trazabilidad`, `view_reportes`, `view_balance`, `admin_users` (solo lectura logs)

---

## Roles Administrativos (Fuera de SoD)

#### 14. **CONFIGURADOR**
- **Función**: Configuración técnica del sistema
- **Puede**: Gestionar usuarios, roles, tipos de artículos, servicios externos
- **NO puede**: Ejecutar operaciones comerciales
- **Permisos**: `admin_users`, `admin_empresa`, `admin_tipos_articulo`, `admin_comprobantes`

#### 15. **SOPORTE_TECNICO**
- **Función**: Responsables de la gestión y mitigación de incidentes y errores del sistema.
- **Puede**: Ver log de errores, dashboard de riesgos y reglas de mitigación técnica.
- **NO puede**: Ejecutar, facturar, aprobar, ni modificar flujos contables normales.
- **Permisos**: `view_error_log`, `view_risk_dashboard`, `view_mitigation_history`, `manage_mitigation_rules`, `dashboard_view`

#### 16. **SYSADMIN**
- **Función**: Superadministrador del sistema
- **Permisos**: `all`, `sysadmin` (acceso total, fuera de controles SoD)

---

## Usuarios de Ejemplo por Rol

```
OPERACIONES:
- usuario: solicita_compras | rol: SOLICITANTE_COMPRAS
- usuario: comprador1       | rol: COMPRADOR
- usuario: aprueba_compras  | rol: APROBADOR_COMPRAS
- usuario: recepcion1       | rol: RECEPCIONISTA
- usuario: vendedor1        | rol: VENDEDOR
- usuario: facturacion1     | rol: FACTURACION
- usuario: almacen1         | rol: ALMACENISTA

TESORERÍA:
- usuario: cuentas_pagar    | rol: CUENTAS_POR_PAGAR
- usuario: autoriza_pagos   | rol: AUTORIZADOR_PAGOS
- usuario: tesorero1        | rol: TESORERO
- usuario: cobranzas1       | rol: COBRANZAS

CONTROL:
- usuario: contador1        | rol: CONTADOR
- usuario: auditor1         | rol: AUDITOR

ADMIN:
- usuario: configurador     | rol: CONFIGURADOR
- usuario: soporte1         | rol: SOPORTE_TECNICO
- usuario: admin            | rol: SYSADMIN (ya existe)
```

---

## Matriz de Control SoD

| Transacción | Solicita | Aprueba | Ejecuta | Registra | Custodia | Concilia | Audita |
|-------------|----------|---------|---------|----------|----------|----------|--------|
| **Compra** | Solicitante | Aprobador | Comprador | Recepción | Almacén | Contador | Auditor |
| **Pago** | Ctas.Pagar | Autorizador | Tesorero | Contador | Tesorero | Contador | Auditor |
| **Venta** | Vendedor | - | Facturación | Facturación | - | Contador | Auditor |
| **Cobranza** | - | - | Cobranzas | Contador | Tesorero | Contador | Auditor |
| **Stock** | Vendedor | - | Almacén | Almacén | Almacén | Contador | Auditor |

✅ **Ninguna persona acumula más de 2 funciones consecutivas en la misma transacción**

---

## Permisos Nuevos Requeridos

Para implementar SoD correctamente, necesitamos agregar:

1. `aprobar_ordenes` - Aprobación gerencial de OC
2. `autorizar_pagos` - Autorización de desembolsos
3. `ejecutar_pagos` - Ejecución de transferencias/cheques
4. Separar `admin_users` en:
   - `admin_users_write` (crear/modificar)
   - `admin_users_read` (solo logs de seguridad)

---

**Referencias**:
- SOX (Sarbanes-Oxley Act) Section 404
- COSO Internal Control Framework
- COBIT 5 - DSS05.04 (Manage User Access)
- ISO 27001 - A.6.1.2 (Segregation of duties)
