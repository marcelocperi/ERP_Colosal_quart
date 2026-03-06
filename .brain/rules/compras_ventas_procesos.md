# Guía Funcional Colosal ERP — Módulos de Compras y Ventas

## MÓDULO COMPRAS — CIRCUITO COMPLETO

PASO 1 — DETECTAR NECESIDAD: Ve a COMPRA → Tablero de Reposición. El sistema muestra artículos bajo el stock mínimo marcados en rojo. Hacé clic en "Generar Pedido" sobre el artículo que necesitás.

PASO 2 — NOTA DE PEDIDO: Ve a COMPRA → Notas de Pedido (NP). Clic en "Nueva Solicitud". Buscá el artículo, ingresá la cantidad y una observación si es urgente. Clic en "Enviar al Gerente".

PASO 3 — APROBACIÓN: El gerente va a COMPRA → Aprobaciones de Compra. Ve las solicitudes pendientes y aprueba o rechaza. Si aprueba, el comprador puede emitir la Orden de Compra.

PASO 4 — ORDEN DE COMPRA: Ve a COMPRA → Nueva Orden de Compra. Elegí al proveedor (el sistema sugiere el último precio). Confirmá cantidades y condiciones de pago. Clic en "Emitir Orden de Compra".

PASO 5 — RECEPCIÓN: Ve a COMPRA → Órdenes de Compra. Buscá la orden, clic en "Registrar Recepción". Indicá cantidades recibidas y el depósito. El stock sube automáticamente.

PASO 6 — FACTURA DEL PROVEEDOR: Ve a COMPRA → Facturar Compras. Seleccioná el proveedor. Ingresá Punto de Venta y Número de factura. Confirmá importes con IVA. El sistema crea la deuda para que Finanzas la pague.

## MÓDULO COMPRAS — CASOS ESPECIALES

CONSIGNACIÓN: El proveedor te deja mercadería sin cobrar. Solo pagás cuando la usás o vendés. Se registra en COMPRA → Facturar Compras seleccionando tipo "Consignación/Tenencia".

IMPORTACIÓN: Los artículos importados quedan en estado "En Tránsito" hasta el despacho de aduana. Los gastos de aduana (derechos, flete, seguro) se suman al costo del artículo. El Demurrage (estadía de contenedor) NO se suma al costo, se imputa como gasto del período.

FAZÓN/TALLER EXTERNO: Si mandás materiales a un tercero para que los procesen, usá COMPRA → Órdenes de Compra con tipo "Fazón". El sistema controla el saldo de material en poder del taller.

RECEPCIÓN A CIEGAS: Si el depósito recibe sin ver la OC, usá COMPRA → Recepción a Ciegas. El sistema cruza automáticamente con la OC pendiente.

## MÓDULO COMPRAS — APROBACIONES Y PROVEEDORES

COTIZAR PRIMERO: Antes de emitir una OC podés pedir precio a varios proveedores. Ve a COMPRA → Cotizaciones. Cargá los artículos y el sistema envía el pedido de cotización a los proveedores seleccionados.

NUEVO PROVEEDOR: Ve a COMPRA → Proveedores → Nuevo. Ingresá CUIT, razón social, dirección y condiciones de pago. El proveedor queda habilitado para recibir órdenes.

VER COMPROBANTES: Ve a COMPRA → Listado Comprobantes. Podés filtrar por proveedor, fecha o estado. Desde ahí también podés anular comprobantes con autorización.

## MÓDULO VENTAS — CIRCUITO COMPLETO

FACTURA DE VENTA: Ve a VENTA → Facturación Electrónica. Seleccioná el cliente. Agregá artículos (buscador o código de barras). El sistema elige automáticamente el tipo: Factura A (cliente IVA Responsable), B (consumidor final) o C (exento). Clic en "Facturar". El sistema se conecta a AFIP y obtiene el CAE. La factura queda autorizada.

CUENTA CORRIENTE: Ve a VENTA → Clientes y abrí la ficha del cliente. En "Cuenta Corriente" ves el saldo adeudado total (facturas menos pagos recibidos).

COBRO DE CLIENTE: Ve a COBRANZAS → Gestión de Cobros. Creá un Recibo a nombre del cliente. Indicá el monto y medio de pago (Efectivo, Cheque, Transferencia, MercadoPago). El sistema descuenta la deuda automáticamente.

## MÓDULO VENTAS — NOTAS DE CRÉDITO Y DEVOLUCIONES

NOTA DE CRÉDITO: Ve a VENTA → Listado Comprobantes. Buscá la factura original. Clic en "Generar Nota de Crédito". Seleccioná los artículos devueltos. Al confirmar: la mercadería vuelve al depósito, la deuda del cliente baja.

CAMBIO CONDICIÓN DE PAGO: Solo gerentes pueden cambiar condiciones. En la ficha del cliente, seleccioná la nueva condición y guardá. Si tu usuario no es gerente, el sistema solicita aprobación automáticamente.

VER FACTURAS EMITIDAS: Ve a VENTA → Listado Comprobantes. Filtrá por fecha, cliente o estado. Desde ahí podés imprimir o enviar por email.

## MÓDULO STOCK — MOVIMIENTOS Y DEPÓSITOS

VER STOCK ACTUAL: Ve a STOCK → Artículos (Maestro). En cada artículo ves el stock actual por depósito.

TRANSFERENCIA ENTRE DEPÓSITOS: Ve a STOCK → Transferencias. Elegí el depósito origen y destino, los artículos y cantidades. Al confirmar, el stock se mueve entre depósitos.

MOVIMIENTOS DE STOCK: Ve a STOCK → Movimientos Stock. Podés ver el historial completo: entradas por compras, salidas por ventas, transferencias y ajustes.

INVENTARIO FÍSICO: Ve a AUDITORIA → Inventarios Físicos. Creá un inventario, recorré el depósito contando artículos y registrá las cantidades reales. El sistema calcula diferencias y genera el ajuste.
