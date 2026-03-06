-- ==========================================
-- COLOSAL ERP EXPERT TUNING SCRIPT
-- Generado basado en patrones reales y 
-- mejores prácticas Multi-Tenant / InnoDB
-- ==========================================


-- 📦 TABLA: clientes
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_clientes_ent_user_id ON clientes (enterprise_id, user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por codigo
CREATE INDEX idx_clientes_ent_codigo ON clientes (enterprise_id, codigo);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por email
CREATE INDEX idx_clientes_ent_email ON clientes (enterprise_id, email);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por cuit
CREATE INDEX idx_clientes_ent_cuit ON clientes (enterprise_id, cuit);

-- 📦 TABLA: cmp_articulos_costos_indirectos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_cmp_articulos_costos_indirectos_ent_articulo_id ON cmp_articulos_costos_indirectos (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_articulos_costos_indirectos_ent_user_id ON cmp_articulos_costos_indirectos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_articulos_costos_indirectos_ent_created_at ON cmp_articulos_costos_indirectos (enterprise_id, created_at);

-- 📦 TABLA: cmp_articulos_proveedores
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por proveedor_id bajo el tenant actual.
CREATE INDEX idx_cmp_articulos_proveedores_ent_proveedor_id ON cmp_articulos_proveedores (enterprise_id, proveedor_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por origen_id bajo el tenant actual.
CREATE INDEX idx_cmp_articulos_proveedores_ent_origen_id ON cmp_articulos_proveedores (enterprise_id, origen_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_articulos_proveedores_ent_user_id ON cmp_articulos_proveedores (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_articulos_proveedores_ent_created_at ON cmp_articulos_proveedores (enterprise_id, created_at);

-- 📦 TABLA: cmp_consignaciones
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_cmp_consignaciones_ent_tercero_id ON cmp_consignaciones (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por deposito_id bajo el tenant actual.
CREATE INDEX idx_cmp_consignaciones_ent_deposito_id ON cmp_consignaciones (enterprise_id, deposito_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_consignaciones_ent_user_id ON cmp_consignaciones (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_consignaciones_ent_created_at ON cmp_consignaciones (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_cmp_consignaciones_ent_estado ON cmp_consignaciones (enterprise_id, estado);

-- 📦 TABLA: cmp_cotizaciones
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por solicitud_origen_id bajo el tenant actual.
CREATE INDEX idx_cmp_cotizaciones_ent_solicitud_origen_id ON cmp_cotizaciones (enterprise_id, solicitud_origen_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por proveedor_id bajo el tenant actual.
CREATE INDEX idx_cmp_cotizaciones_ent_proveedor_id ON cmp_cotizaciones (enterprise_id, proveedor_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_cotizaciones_ent_user_id ON cmp_cotizaciones (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_cotizaciones_ent_created_at ON cmp_cotizaciones (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_cmp_cotizaciones_ent_estado ON cmp_cotizaciones (enterprise_id, estado);

-- 📦 TABLA: cmp_detalles_orden
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_id bajo el tenant actual.
CREATE INDEX idx_cmp_detalles_orden_ent_orden_id ON cmp_detalles_orden (enterprise_id, orden_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_cmp_detalles_orden_ent_articulo_id ON cmp_detalles_orden (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_detalles_orden_ent_user_id ON cmp_detalles_orden (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_detalles_orden_ent_created_at ON cmp_detalles_orden (enterprise_id, created_at);

-- 📦 TABLA: cmp_detalles_solicitud
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por solicitud_id bajo el tenant actual.
CREATE INDEX idx_cmp_detalles_solicitud_ent_solicitud_id ON cmp_detalles_solicitud (enterprise_id, solicitud_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_cmp_detalles_solicitud_ent_articulo_id ON cmp_detalles_solicitud (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_detalles_solicitud_ent_user_id ON cmp_detalles_solicitud (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_detalles_solicitud_ent_created_at ON cmp_detalles_solicitud (enterprise_id, created_at);

-- 📦 TABLA: cmp_items_consignacion
-- Razón: Falta índice FK en moneda_id crítico para JOIN performance.
CREATE INDEX idx_cmp_items_consignacion_moneda_id ON cmp_items_consignacion (moneda_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_cmp_items_consignacion_user_id ON cmp_items_consignacion (user_id);

-- 📦 TABLA: cmp_items_cotizacion
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cotizacion_id bajo el tenant actual.
CREATE INDEX idx_cmp_items_cotizacion_ent_cotizacion_id ON cmp_items_cotizacion (enterprise_id, cotizacion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_cmp_items_cotizacion_ent_articulo_id ON cmp_items_cotizacion (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_items_cotizacion_ent_user_id ON cmp_items_cotizacion (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_items_cotizacion_ent_created_at ON cmp_items_cotizacion (enterprise_id, created_at);

-- 📦 TABLA: cmp_liquidaciones_consignacion
-- Razón: Falta índice FK en comprobante_id crítico para JOIN performance.
CREATE INDEX idx_cmp_liquidaciones_consignacion_comprobante_id ON cmp_liquidaciones_consignacion (comprobante_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_cmp_liquidaciones_consignacion_user_id ON cmp_liquidaciones_consignacion (user_id);

-- 📦 TABLA: cmp_ordenes_compra
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por proveedor_id bajo el tenant actual.
CREATE INDEX idx_cmp_ordenes_compra_ent_proveedor_id ON cmp_ordenes_compra (enterprise_id, proveedor_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cotizacion_id bajo el tenant actual.
CREATE INDEX idx_cmp_ordenes_compra_ent_cotizacion_id ON cmp_ordenes_compra (enterprise_id, cotizacion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por aprobador_compras_id bajo el tenant actual.
CREATE INDEX idx_cmp_ordenes_compra_ent_aprobador_compras_id ON cmp_ordenes_compra (enterprise_id, aprobador_compras_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por aprobador_tesoreria_id bajo el tenant actual.
CREATE INDEX idx_cmp_ordenes_compra_ent_aprobador_tesoreria_id ON cmp_ordenes_compra (enterprise_id, aprobador_tesoreria_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_ordenes_compra_ent_user_id ON cmp_ordenes_compra (enterprise_id, user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por centro_costo_id bajo el tenant actual.
CREATE INDEX idx_cmp_ordenes_compra_ent_centro_costo_id ON cmp_ordenes_compra (enterprise_id, centro_costo_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha en el tenant.
CREATE INDEX idx_cmp_ordenes_compra_ent_fecha ON cmp_ordenes_compra (enterprise_id, fecha);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha_emision en el tenant.
CREATE INDEX idx_cmp_ordenes_compra_ent_fecha_emision ON cmp_ordenes_compra (enterprise_id, fecha_emision);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_ordenes_compra_ent_created_at ON cmp_ordenes_compra (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_cmp_ordenes_compra_ent_estado ON cmp_ordenes_compra (enterprise_id, estado);

-- 📦 TABLA: cmp_overhead_templates
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_overhead_templates_ent_user_id ON cmp_overhead_templates (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_overhead_templates_ent_created_at ON cmp_overhead_templates (enterprise_id, created_at);

-- 📦 TABLA: cmp_overhead_templates_detalle
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por template_id bajo el tenant actual.
CREATE INDEX idx_cmp_overhead_templates_detalle_ent_template_id ON cmp_overhead_templates_detalle (enterprise_id, template_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_overhead_templates_detalle_ent_user_id ON cmp_overhead_templates_detalle (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_overhead_templates_detalle_ent_created_at ON cmp_overhead_templates_detalle (enterprise_id, created_at);

-- 📦 TABLA: cmp_recetas_bom
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por producto_id bajo el tenant actual.
CREATE INDEX idx_cmp_recetas_bom_ent_producto_id ON cmp_recetas_bom (enterprise_id, producto_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_recetas_bom_ent_user_id ON cmp_recetas_bom (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_recetas_bom_ent_created_at ON cmp_recetas_bom (enterprise_id, created_at);

-- 📦 TABLA: cmp_recetas_detalle
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_cmp_recetas_detalle_user_id ON cmp_recetas_detalle (user_id);

-- 📦 TABLA: cmp_rfq_campanas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_objetivo_id bajo el tenant actual.
CREATE INDEX idx_cmp_rfq_campanas_ent_articulo_objetivo_id ON cmp_rfq_campanas (enterprise_id, articulo_objetivo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_rfq_campanas_ent_user_id ON cmp_rfq_campanas (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha_emision en el tenant.
CREATE INDEX idx_cmp_rfq_campanas_ent_fecha_emision ON cmp_rfq_campanas (enterprise_id, fecha_emision);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_rfq_campanas_ent_created_at ON cmp_rfq_campanas (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_cmp_rfq_campanas_ent_estado ON cmp_rfq_campanas (enterprise_id, estado);

-- 📦 TABLA: cmp_rfq_cotizaciones
-- Razón: Falta índice FK en proveedor_id crítico para JOIN performance.
CREATE INDEX idx_cmp_rfq_cotizaciones_proveedor_id ON cmp_rfq_cotizaciones (proveedor_id);

-- 📦 TABLA: cmp_rfq_detalles
-- Razón: Falta índice FK en articulo_insumo_id crítico para JOIN performance.
CREATE INDEX idx_cmp_rfq_detalles_articulo_insumo_id ON cmp_rfq_detalles (articulo_insumo_id);

-- 📦 TABLA: cmp_solicitudes_reposicion
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por solicitante_id bajo el tenant actual.
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_solicitante_id ON cmp_solicitudes_reposicion (enterprise_id, solicitante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por aprobador_id bajo el tenant actual.
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_aprobador_id ON cmp_solicitudes_reposicion (enterprise_id, aprobador_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_user_id ON cmp_solicitudes_reposicion (enterprise_id, user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por centro_costo_id bajo el tenant actual.
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_centro_costo_id ON cmp_solicitudes_reposicion (enterprise_id, centro_costo_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha en el tenant.
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_fecha ON cmp_solicitudes_reposicion (enterprise_id, fecha);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_created_at ON cmp_solicitudes_reposicion (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_estado ON cmp_solicitudes_reposicion (enterprise_id, estado);

-- 📦 TABLA: cmp_sourcing_origenes
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cmp_sourcing_origenes_ent_user_id ON cmp_sourcing_origenes (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_cmp_sourcing_origenes_ent_created_at ON cmp_sourcing_origenes (enterprise_id, created_at);

-- 📦 TABLA: cont_asientos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_cont_asientos_ent_comprobante_id ON cont_asientos (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cont_asientos_ent_user_id ON cont_asientos (enterprise_id, user_id);

-- 📦 TABLA: cont_asientos_detalle
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por asiento_id bajo el tenant actual.
CREATE INDEX idx_cont_asientos_detalle_ent_asiento_id ON cont_asientos_detalle (enterprise_id, asiento_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cuenta_id bajo el tenant actual.
CREATE INDEX idx_cont_asientos_detalle_ent_cuenta_id ON cont_asientos_detalle (enterprise_id, cuenta_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cont_asientos_detalle_ent_user_id ON cont_asientos_detalle (enterprise_id, user_id);

-- 📦 TABLA: cont_plan_cuentas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por padre_id bajo el tenant actual.
CREATE INDEX idx_cont_plan_cuentas_ent_padre_id ON cont_plan_cuentas (enterprise_id, padre_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cont_plan_cuentas_ent_user_id ON cont_plan_cuentas (enterprise_id, user_id);

-- 📦 TABLA: cotizacion_dolar
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_cotizacion_dolar_ent_user_id ON cotizacion_dolar (enterprise_id, user_id);

-- 📦 TABLA: enterprises
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_enterprises_user_id ON enterprises (user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por cuit
CREATE INDEX idx_enterprises_cuit ON enterprises (cuit);

-- 📦 TABLA: erp_areas
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_areas_ent_created_at ON erp_areas (enterprise_id, created_at);

-- 📦 TABLA: erp_comprobantes
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_tercero_id ON erp_comprobantes (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por jurisdiccion_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_jurisdiccion_id ON erp_comprobantes (enterprise_id, jurisdiccion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por asiento_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_asiento_id ON erp_comprobantes (enterprise_id, asiento_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_asociado_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_comprobante_asociado_id ON erp_comprobantes (enterprise_id, comprobante_asociado_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por direccion_entrega_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_direccion_entrega_id ON erp_comprobantes (enterprise_id, direccion_entrega_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por receptor_contacto_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_receptor_contacto_id ON erp_comprobantes (enterprise_id, receptor_contacto_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por condicion_pago_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_condicion_pago_id ON erp_comprobantes (enterprise_id, condicion_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por logistica_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_logistica_id ON erp_comprobantes (enterprise_id, logistica_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_ent_user_id ON erp_comprobantes (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha_emision en el tenant.
CREATE INDEX idx_erp_comprobantes_ent_fecha_emision ON erp_comprobantes (enterprise_id, fecha_emision);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_comprobantes_ent_created_at ON erp_comprobantes (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero
CREATE INDEX idx_erp_comprobantes_ent_numero ON erp_comprobantes (enterprise_id, numero);

-- 📦 TABLA: erp_comprobantes_detalle
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_detalle_ent_comprobante_id ON erp_comprobantes_detalle (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_detalle_ent_articulo_id ON erp_comprobantes_detalle (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_detalle_ent_user_id ON erp_comprobantes_detalle (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_comprobantes_detalle_ent_created_at ON erp_comprobantes_detalle (enterprise_id, created_at);

-- 📦 TABLA: erp_comprobantes_impuestos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_impuestos_ent_comprobante_id ON erp_comprobantes_impuestos (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por jurisdiccion_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_impuestos_ent_jurisdiccion_id ON erp_comprobantes_impuestos (enterprise_id, jurisdiccion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por impuesto_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_impuestos_ent_impuesto_id ON erp_comprobantes_impuestos (enterprise_id, impuesto_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_comprobantes_impuestos_ent_user_id ON erp_comprobantes_impuestos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_comprobantes_impuestos_ent_created_at ON erp_comprobantes_impuestos (enterprise_id, created_at);

-- 📦 TABLA: erp_contactos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_erp_contactos_ent_tercero_id ON erp_contactos (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por puesto_id bajo el tenant actual.
CREATE INDEX idx_erp_contactos_ent_puesto_id ON erp_contactos (enterprise_id, puesto_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por direccion_id bajo el tenant actual.
CREATE INDEX idx_erp_contactos_ent_direccion_id ON erp_contactos (enterprise_id, direccion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_contactos_ent_user_id ON erp_contactos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_contactos_ent_created_at ON erp_contactos (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por email
CREATE INDEX idx_erp_contactos_ent_email ON erp_contactos (enterprise_id, email);

-- 📦 TABLA: erp_cuentas_fondos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cuenta_contable_id bajo el tenant actual.
CREATE INDEX idx_erp_cuentas_fondos_ent_cuenta_contable_id ON erp_cuentas_fondos (enterprise_id, cuenta_contable_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_cuentas_fondos_ent_user_id ON erp_cuentas_fondos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_cuentas_fondos_ent_created_at ON erp_cuentas_fondos (enterprise_id, created_at);

-- 📦 TABLA: erp_datos_fiscales
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_erp_datos_fiscales_ent_tercero_id ON erp_datos_fiscales (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_datos_fiscales_ent_user_id ON erp_datos_fiscales (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_datos_fiscales_ent_created_at ON erp_datos_fiscales (enterprise_id, created_at);

-- 📦 TABLA: erp_direcciones
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_erp_direcciones_ent_tercero_id ON erp_direcciones (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_direcciones_ent_user_id ON erp_direcciones (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_direcciones_ent_created_at ON erp_direcciones (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero
CREATE INDEX idx_erp_direcciones_ent_numero ON erp_direcciones (enterprise_id, numero);

-- 📦 TABLA: erp_movimientos_fondos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_erp_movimientos_fondos_ent_tercero_id ON erp_movimientos_fondos (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cuenta_fondo_id bajo el tenant actual.
CREATE INDEX idx_erp_movimientos_fondos_ent_cuenta_fondo_id ON erp_movimientos_fondos (enterprise_id, cuenta_fondo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_asociado_id bajo el tenant actual.
CREATE INDEX idx_erp_movimientos_fondos_ent_comprobante_asociado_id ON erp_movimientos_fondos (enterprise_id, comprobante_asociado_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por asiento_id bajo el tenant actual.
CREATE INDEX idx_erp_movimientos_fondos_ent_asiento_id ON erp_movimientos_fondos (enterprise_id, asiento_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_movimientos_fondos_ent_user_id ON erp_movimientos_fondos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha en el tenant.
CREATE INDEX idx_erp_movimientos_fondos_ent_fecha ON erp_movimientos_fondos (enterprise_id, fecha);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_movimientos_fondos_ent_created_at ON erp_movimientos_fondos (enterprise_id, created_at);

-- 📦 TABLA: erp_puestos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_puestos_ent_user_id ON erp_puestos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_puestos_ent_created_at ON erp_puestos (enterprise_id, created_at);

-- 📦 TABLA: erp_terceros
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por condicion_pago_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_ent_condicion_pago_id ON erp_terceros (enterprise_id, condicion_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por condicion_pago_pendiente_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_ent_condicion_pago_pendiente_id ON erp_terceros (enterprise_id, condicion_pago_pendiente_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por condicion_mixta_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_ent_condicion_mixta_id ON erp_terceros (enterprise_id, condicion_mixta_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_ent_user_id ON erp_terceros (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_terceros_ent_created_at ON erp_terceros (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por codigo
CREATE INDEX idx_erp_terceros_ent_codigo ON erp_terceros (enterprise_id, codigo);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por email
CREATE INDEX idx_erp_terceros_ent_email ON erp_terceros (enterprise_id, email);

-- 📦 TABLA: erp_terceros_cm05
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_cm05_ent_user_id ON erp_terceros_cm05 (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_terceros_cm05_ent_created_at ON erp_terceros_cm05 (enterprise_id, created_at);

-- 📦 TABLA: erp_terceros_condiciones
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por condicion_pago_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_condiciones_ent_condicion_pago_id ON erp_terceros_condiciones (enterprise_id, condicion_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_condiciones_ent_user_id ON erp_terceros_condiciones (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_terceros_condiciones_ent_created_at ON erp_terceros_condiciones (enterprise_id, created_at);

-- 📦 TABLA: erp_terceros_jurisdicciones
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_erp_terceros_jurisdicciones_ent_user_id ON erp_terceros_jurisdicciones (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_erp_terceros_jurisdicciones_ent_created_at ON erp_terceros_jurisdicciones (enterprise_id, created_at);

-- 📦 TABLA: fin_bancos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por bcra_id bajo el tenant actual.
CREATE INDEX idx_fin_bancos_ent_bcra_id ON fin_bancos (enterprise_id, bcra_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cuenta_contable_id bajo el tenant actual.
CREATE INDEX idx_fin_bancos_ent_cuenta_contable_id ON fin_bancos (enterprise_id, cuenta_contable_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_bancos_ent_user_id ON fin_bancos (enterprise_id, user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por cuit
CREATE INDEX idx_fin_bancos_ent_cuit ON fin_bancos (enterprise_id, cuit);

-- 📦 TABLA: fin_cheques
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por banco_id bajo el tenant actual.
CREATE INDEX idx_fin_cheques_ent_banco_id ON fin_cheques (enterprise_id, banco_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_fin_cheques_ent_tercero_id ON fin_cheques (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por recibo_origen_id bajo el tenant actual.
CREATE INDEX idx_fin_cheques_ent_recibo_origen_id ON fin_cheques (enterprise_id, recibo_origen_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_pago_destino_id bajo el tenant actual.
CREATE INDEX idx_fin_cheques_ent_orden_pago_destino_id ON fin_cheques (enterprise_id, orden_pago_destino_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_cheques_ent_user_id ON fin_cheques (enterprise_id, user_id);

-- 📦 TABLA: fin_condiciones_pago
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_condiciones_pago_ent_user_id ON fin_condiciones_pago (enterprise_id, user_id);

-- 📦 TABLA: fin_condiciones_pago_mixtas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_condiciones_pago_mixtas_ent_user_id ON fin_condiciones_pago_mixtas (enterprise_id, user_id);

-- 📦 TABLA: fin_condiciones_pago_mixtas_detalle
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por mixta_id bajo el tenant actual.
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_ent_mixta_id ON fin_condiciones_pago_mixtas_detalle (enterprise_id, mixta_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por condicion_pago_id bajo el tenant actual.
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_ent_condicion_pago_id ON fin_condiciones_pago_mixtas_detalle (enterprise_id, condicion_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_ent_user_id ON fin_condiciones_pago_mixtas_detalle (enterprise_id, user_id);

-- 📦 TABLA: fin_devoluciones_valores
-- Razón: Falta índice FK en medio_pago_id crítico para JOIN performance.
CREATE INDEX idx_fin_devoluciones_valores_medio_pago_id ON fin_devoluciones_valores (medio_pago_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_fin_devoluciones_valores_user_id ON fin_devoluciones_valores (user_id);

-- 📦 TABLA: fin_factura_cobros
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por factura_id bajo el tenant actual.
CREATE INDEX idx_fin_factura_cobros_ent_factura_id ON fin_factura_cobros (enterprise_id, factura_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por medio_pago_id bajo el tenant actual.
CREATE INDEX idx_fin_factura_cobros_ent_medio_pago_id ON fin_factura_cobros (enterprise_id, medio_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cuenta_contable_snapshot_id bajo el tenant actual.
CREATE INDEX idx_fin_factura_cobros_ent_cuenta_contable_snapshot_id ON fin_factura_cobros (enterprise_id, cuenta_contable_snapshot_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_factura_cobros_ent_user_id ON fin_factura_cobros (enterprise_id, user_id);

-- 📦 TABLA: fin_liquidaciones
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por nomina_id bajo el tenant actual.
CREATE INDEX idx_fin_liquidaciones_ent_nomina_id ON fin_liquidaciones (enterprise_id, nomina_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por usuario_id bajo el tenant actual.
CREATE INDEX idx_fin_liquidaciones_ent_usuario_id ON fin_liquidaciones (enterprise_id, usuario_id);

-- 📦 TABLA: fin_medios_pago
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cuenta_contable_id bajo el tenant actual.
CREATE INDEX idx_fin_medios_pago_ent_cuenta_contable_id ON fin_medios_pago (enterprise_id, cuenta_contable_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_medios_pago_ent_user_id ON fin_medios_pago (enterprise_id, user_id);

-- 📦 TABLA: fin_nominas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por asiento_id bajo el tenant actual.
CREATE INDEX idx_fin_nominas_ent_asiento_id ON fin_nominas (enterprise_id, asiento_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_nominas_ent_user_id ON fin_nominas (enterprise_id, user_id);

-- 📦 TABLA: fin_ordenes_pago
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_ent_tercero_id ON fin_ordenes_pago (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_ent_user_id ON fin_ordenes_pago (enterprise_id, user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero
CREATE INDEX idx_fin_ordenes_pago_ent_numero ON fin_ordenes_pago (enterprise_id, numero);

-- 📦 TABLA: fin_ordenes_pago_comprobantes
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_pago_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_comprobantes_ent_orden_pago_id ON fin_ordenes_pago_comprobantes (enterprise_id, orden_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_comprobantes_ent_comprobante_id ON fin_ordenes_pago_comprobantes (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_comprobantes_ent_user_id ON fin_ordenes_pago_comprobantes (enterprise_id, user_id);

-- 📦 TABLA: fin_ordenes_pago_medios
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_pago_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_medios_ent_orden_pago_id ON fin_ordenes_pago_medios (enterprise_id, orden_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por medio_pago_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_medios_ent_medio_pago_id ON fin_ordenes_pago_medios (enterprise_id, medio_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cuenta_contable_snapshot_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_medios_ent_cuenta_contable_snapshot_id ON fin_ordenes_pago_medios (enterprise_id, cuenta_contable_snapshot_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por debin_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_medios_ent_debin_id ON fin_ordenes_pago_medios (enterprise_id, debin_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por banco_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_medios_ent_banco_id ON fin_ordenes_pago_medios (enterprise_id, banco_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_ordenes_pago_medios_ent_user_id ON fin_ordenes_pago_medios (enterprise_id, user_id);

-- 📦 TABLA: fin_recibos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_fin_recibos_ent_tercero_id ON fin_recibos (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_recibos_ent_user_id ON fin_recibos (enterprise_id, user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por asiento_id bajo el tenant actual.
CREATE INDEX idx_fin_recibos_ent_asiento_id ON fin_recibos (enterprise_id, asiento_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero
CREATE INDEX idx_fin_recibos_ent_numero ON fin_recibos (enterprise_id, numero);

-- 📦 TABLA: fin_recibos_detalles
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_fin_recibos_detalles_user_id ON fin_recibos_detalles (user_id);

-- 📦 TABLA: fin_recibos_medios
-- Razón: Falta índice FK en medio_pago_id crítico para JOIN performance.
CREATE INDEX idx_fin_recibos_medios_medio_pago_id ON fin_recibos_medios (medio_pago_id);
-- Razón: Falta índice FK en cuenta_contable_snapshot_id crítico para JOIN performance.
CREATE INDEX idx_fin_recibos_medios_cuenta_contable_snapshot_id ON fin_recibos_medios (cuenta_contable_snapshot_id);
-- Razón: Falta índice FK en banco_id crítico para JOIN performance.
CREATE INDEX idx_fin_recibos_medios_banco_id ON fin_recibos_medios (banco_id);
-- Razón: Falta índice FK en debin_id crítico para JOIN performance.
CREATE INDEX idx_fin_recibos_medios_debin_id ON fin_recibos_medios (debin_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_fin_recibos_medios_user_id ON fin_recibos_medios (user_id);

-- 📦 TABLA: fin_retenciones_emitidas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_pago_id bajo el tenant actual.
CREATE INDEX idx_fin_retenciones_emitidas_ent_comprobante_pago_id ON fin_retenciones_emitidas (enterprise_id, comprobante_pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por jurisdiccion_id bajo el tenant actual.
CREATE INDEX idx_fin_retenciones_emitidas_ent_jurisdiccion_id ON fin_retenciones_emitidas (enterprise_id, jurisdiccion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_fin_retenciones_emitidas_ent_tercero_id ON fin_retenciones_emitidas (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_retenciones_emitidas_ent_user_id ON fin_retenciones_emitidas (enterprise_id, user_id);

-- 📦 TABLA: fin_tipos_cambio
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_fin_tipos_cambio_ent_user_id ON fin_tipos_cambio (enterprise_id, user_id);

-- 📦 TABLA: historial_prestamos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por usuario_id bajo el tenant actual.
CREATE INDEX idx_historial_prestamos_ent_usuario_id ON historial_prestamos (enterprise_id, usuario_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por libro_id bajo el tenant actual.
CREATE INDEX idx_historial_prestamos_ent_libro_id ON historial_prestamos (enterprise_id, libro_id);

-- 📦 TABLA: imp_cargos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_compra_id bajo el tenant actual.
CREATE INDEX idx_imp_cargos_ent_orden_compra_id ON imp_cargos (enterprise_id, orden_compra_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por proveedor_id bajo el tenant actual.
CREATE INDEX idx_imp_cargos_ent_proveedor_id ON imp_cargos (enterprise_id, proveedor_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_imp_cargos_ent_comprobante_id ON imp_cargos (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_imp_cargos_ent_user_id ON imp_cargos (enterprise_id, user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por despacho_id bajo el tenant actual.
CREATE INDEX idx_imp_cargos_ent_despacho_id ON imp_cargos (enterprise_id, despacho_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por pago_id bajo el tenant actual.
CREATE INDEX idx_imp_cargos_ent_pago_id ON imp_cargos (enterprise_id, pago_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por cargo_referencia_id bajo el tenant actual.
CREATE INDEX idx_imp_cargos_ent_cargo_referencia_id ON imp_cargos (enterprise_id, cargo_referencia_id);

-- 📦 TABLA: imp_despachos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_compra_id bajo el tenant actual.
CREATE INDEX idx_imp_despachos_ent_orden_compra_id ON imp_despachos (enterprise_id, orden_compra_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por despachante_id bajo el tenant actual.
CREATE INDEX idx_imp_despachos_ent_despachante_id ON imp_despachos (enterprise_id, despachante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_imp_despachos_ent_user_id ON imp_despachos (enterprise_id, user_id);

-- 📦 TABLA: imp_despachos_items
-- Razón: Falta índice FK en orden_compra_id crítico para JOIN performance.
CREATE INDEX idx_imp_despachos_items_orden_compra_id ON imp_despachos_items (orden_compra_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_imp_despachos_items_user_id ON imp_despachos_items (user_id);

-- 📦 TABLA: imp_documentos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_compra_id bajo el tenant actual.
CREATE INDEX idx_imp_documentos_ent_orden_compra_id ON imp_documentos (enterprise_id, orden_compra_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por proveedor_id bajo el tenant actual.
CREATE INDEX idx_imp_documentos_ent_proveedor_id ON imp_documentos (enterprise_id, proveedor_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_imp_documentos_ent_user_id ON imp_documentos (enterprise_id, user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por despacho_id bajo el tenant actual.
CREATE INDEX idx_imp_documentos_ent_despacho_id ON imp_documentos (enterprise_id, despacho_id);

-- 📦 TABLA: imp_pagos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_compra_id bajo el tenant actual.
CREATE INDEX idx_imp_pagos_ent_orden_compra_id ON imp_pagos (enterprise_id, orden_compra_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por proveedor_id bajo el tenant actual.
CREATE INDEX idx_imp_pagos_ent_proveedor_id ON imp_pagos (enterprise_id, proveedor_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por banco_id bajo el tenant actual.
CREATE INDEX idx_imp_pagos_ent_banco_id ON imp_pagos (enterprise_id, banco_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por asiento_id bajo el tenant actual.
CREATE INDEX idx_imp_pagos_ent_asiento_id ON imp_pagos (enterprise_id, asiento_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_imp_pagos_ent_user_id ON imp_pagos (enterprise_id, user_id);

-- 📦 TABLA: imp_vessel_tracking
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_compra_id bajo el tenant actual.
CREATE INDEX idx_imp_vessel_tracking_ent_orden_compra_id ON imp_vessel_tracking (enterprise_id, orden_compra_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_imp_vessel_tracking_ent_user_id ON imp_vessel_tracking (enterprise_id, user_id);

-- 📦 TABLA: legacy_libros
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_legacy_libros_ent_user_id ON legacy_libros (enterprise_id, user_id);

-- 📦 TABLA: libros_detalles
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por libro_id bajo el tenant actual.
CREATE INDEX idx_libros_detalles_ent_libro_id ON libros_detalles (enterprise_id, libro_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_libros_detalles_ent_user_id ON libros_detalles (enterprise_id, user_id);

-- 📦 TABLA: log_erp_terceros_cm05
-- Razón: Falta índice FK en tercero_id crítico para JOIN performance.
CREATE INDEX idx_log_erp_terceros_cm05_tercero_id ON log_erp_terceros_cm05 (tercero_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_log_erp_terceros_cm05_user_id ON log_erp_terceros_cm05 (user_id);

-- 📦 TABLA: movimientos_pendientes
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por libro_id bajo el tenant actual.
CREATE INDEX idx_movimientos_pendientes_ent_libro_id ON movimientos_pendientes (enterprise_id, libro_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por motivo_id bajo el tenant actual.
CREATE INDEX idx_movimientos_pendientes_ent_motivo_id ON movimientos_pendientes (enterprise_id, motivo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_movimientos_pendientes_ent_user_id ON movimientos_pendientes (enterprise_id, user_id);

-- 📦 TABLA: movimientos_stock
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_movimientos_stock_ent_articulo_id ON movimientos_stock (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por usuario_id bajo el tenant actual.
CREATE INDEX idx_movimientos_stock_ent_usuario_id ON movimientos_stock (enterprise_id, usuario_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por documento_id bajo el tenant actual.
CREATE INDEX idx_movimientos_stock_ent_documento_id ON movimientos_stock (enterprise_id, documento_id);

-- 📦 TABLA: prd_proyectos_desarrollo
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_objetivo_id bajo el tenant actual.
CREATE INDEX idx_prd_proyectos_desarrollo_ent_articulo_objetivo_id ON prd_proyectos_desarrollo (enterprise_id, articulo_objetivo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_prd_proyectos_desarrollo_ent_user_id ON prd_proyectos_desarrollo (enterprise_id, user_id);

-- 📦 TABLA: prestamos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por usuario_id bajo el tenant actual.
CREATE INDEX idx_prestamos_ent_usuario_id ON prestamos (enterprise_id, usuario_id);

-- 📦 TABLA: proveedores
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_proveedores_ent_user_id ON proveedores (enterprise_id, user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por codigo
CREATE INDEX idx_proveedores_ent_codigo ON proveedores (enterprise_id, codigo);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por email
CREATE INDEX idx_proveedores_ent_email ON proveedores (enterprise_id, email);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por cuit
CREATE INDEX idx_proveedores_ent_cuit ON proveedores (enterprise_id, cuit);

-- 📦 TABLA: service_efficiency
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_service_efficiency_user_id ON service_efficiency (user_id);

-- 📦 TABLA: stk_archivos_digitales
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_archivos_digitales_ent_articulo_id ON stk_archivos_digitales (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_archivos_digitales_ent_user_id ON stk_archivos_digitales (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_archivos_digitales_ent_created_at ON stk_archivos_digitales (enterprise_id, created_at);

-- 📦 TABLA: stk_articulos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por categoria_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_ent_categoria_id ON stk_articulos (enterprise_id, categoria_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tipo_articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_ent_tipo_articulo_id ON stk_articulos (enterprise_id, tipo_articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_ent_user_id ON stk_articulos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_articulos_ent_created_at ON stk_articulos (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por codigo
CREATE INDEX idx_stk_articulos_ent_codigo ON stk_articulos (enterprise_id, codigo);

-- 📦 TABLA: stk_articulos_codigos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_codigos_ent_articulo_id ON stk_articulos_codigos (enterprise_id, articulo_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_articulos_codigos_ent_created_at ON stk_articulos_codigos (enterprise_id, created_at);

-- 📦 TABLA: stk_articulos_precios
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_precios_ent_articulo_id ON stk_articulos_precios (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por lista_precio_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_precios_ent_lista_precio_id ON stk_articulos_precios (enterprise_id, lista_precio_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por regla_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_precios_ent_regla_id ON stk_articulos_precios (enterprise_id, regla_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_precios_ent_user_id ON stk_articulos_precios (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_articulos_precios_ent_created_at ON stk_articulos_precios (enterprise_id, created_at);

-- 📦 TABLA: stk_articulos_seguridad
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_articulos_seguridad_ent_articulo_id ON stk_articulos_seguridad (enterprise_id, articulo_id);

-- 📦 TABLA: stk_balanzas_config
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_balanzas_config_ent_created_at ON stk_balanzas_config (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero_serie
CREATE INDEX idx_stk_balanzas_config_ent_numero_serie ON stk_balanzas_config (enterprise_id, numero_serie);

-- 📦 TABLA: stk_barcode_rules
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_barcode_rules_ent_created_at ON stk_barcode_rules (enterprise_id, created_at);

-- 📦 TABLA: stk_depositos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_stk_depositos_ent_tercero_id ON stk_depositos (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_depositos_ent_user_id ON stk_depositos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_depositos_ent_created_at ON stk_depositos (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero
CREATE INDEX idx_stk_depositos_ent_numero ON stk_depositos (enterprise_id, numero);

-- 📦 TABLA: stk_detalles_recepcion
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por recepcion_id bajo el tenant actual.
CREATE INDEX idx_stk_detalles_recepcion_ent_recepcion_id ON stk_detalles_recepcion (enterprise_id, recepcion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por detalle_orden_id bajo el tenant actual.
CREATE INDEX idx_stk_detalles_recepcion_ent_detalle_orden_id ON stk_detalles_recepcion (enterprise_id, detalle_orden_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_detalles_recepcion_ent_articulo_id ON stk_detalles_recepcion (enterprise_id, articulo_id);

-- 📦 TABLA: stk_devoluciones_solicitudes
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_tercero_id ON stk_devoluciones_solicitudes (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_origen_id bajo el tenant actual.
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_comprobante_origen_id ON stk_devoluciones_solicitudes (enterprise_id, comprobante_origen_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por deposito_destino_id bajo el tenant actual.
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_deposito_destino_id ON stk_devoluciones_solicitudes (enterprise_id, deposito_destino_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por logistica_id bajo el tenant actual.
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_logistica_id ON stk_devoluciones_solicitudes (enterprise_id, logistica_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por condicion_devolucion_id bajo el tenant actual.
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_condicion_devolucion_id ON stk_devoluciones_solicitudes (enterprise_id, condicion_devolucion_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_user_id ON stk_devoluciones_solicitudes (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_created_at ON stk_devoluciones_solicitudes (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_estado ON stk_devoluciones_solicitudes (enterprise_id, estado);

-- 📦 TABLA: stk_devoluciones_solicitudes_det
-- Razón: Falta índice FK en articulo_id crítico para JOIN performance.
CREATE INDEX idx_stk_devoluciones_solicitudes_det_articulo_id ON stk_devoluciones_solicitudes_det (articulo_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_stk_devoluciones_solicitudes_det_user_id ON stk_devoluciones_solicitudes_det (user_id);

-- 📦 TABLA: stk_existencias
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_existencias_ent_user_id ON stk_existencias (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_existencias_ent_created_at ON stk_existencias (enterprise_id, created_at);

-- 📦 TABLA: stk_impresoras_config
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_impresoras_config_ent_created_at ON stk_impresoras_config (enterprise_id, created_at);

-- 📦 TABLA: stk_inventarios
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por deposito_id bajo el tenant actual.
CREATE INDEX idx_stk_inventarios_ent_deposito_id ON stk_inventarios (enterprise_id, deposito_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por responsable_id bajo el tenant actual.
CREATE INDEX idx_stk_inventarios_ent_responsable_id ON stk_inventarios (enterprise_id, responsable_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_inventarios_ent_user_id ON stk_inventarios (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_inventarios_ent_created_at ON stk_inventarios (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_inventarios_ent_estado ON stk_inventarios (enterprise_id, estado);

-- 📦 TABLA: stk_items_inventario
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por inventario_id bajo el tenant actual.
CREATE INDEX idx_stk_items_inventario_ent_inventario_id ON stk_items_inventario (enterprise_id, inventario_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_items_inventario_ent_articulo_id ON stk_items_inventario (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_items_inventario_ent_user_id ON stk_items_inventario (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_items_inventario_ent_created_at ON stk_items_inventario (enterprise_id, created_at);

-- 📦 TABLA: stk_items_transferencia
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por transferencia_id bajo el tenant actual.
CREATE INDEX idx_stk_items_transferencia_ent_transferencia_id ON stk_items_transferencia (enterprise_id, transferencia_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_items_transferencia_ent_articulo_id ON stk_items_transferencia (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_items_transferencia_ent_user_id ON stk_items_transferencia (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_items_transferencia_ent_created_at ON stk_items_transferencia (enterprise_id, created_at);

-- 📦 TABLA: stk_liquidaciones_consignacion
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_tercero_id ON stk_liquidaciones_consignacion (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por deposito_id bajo el tenant actual.
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_deposito_id ON stk_liquidaciones_consignacion (enterprise_id, deposito_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_comprobante_id ON stk_liquidaciones_consignacion (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_user_id ON stk_liquidaciones_consignacion (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_created_at ON stk_liquidaciones_consignacion (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_estado ON stk_liquidaciones_consignacion (enterprise_id, estado);

-- 📦 TABLA: stk_liquidaciones_consignacion_det
-- Razón: Falta índice FK en articulo_id crítico para JOIN performance.
CREATE INDEX idx_stk_liquidaciones_consignacion_det_articulo_id ON stk_liquidaciones_consignacion_det (articulo_id);

-- 📦 TABLA: stk_listas_precios
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_listas_precios_ent_created_at ON stk_listas_precios (enterprise_id, created_at);

-- 📦 TABLA: stk_logisticas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_logisticas_ent_user_id ON stk_logisticas (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_logisticas_ent_created_at ON stk_logisticas (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero
CREATE INDEX idx_stk_logisticas_ent_numero ON stk_logisticas (enterprise_id, numero);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por email
CREATE INDEX idx_stk_logisticas_ent_email ON stk_logisticas (enterprise_id, email);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por cuit
CREATE INDEX idx_stk_logisticas_ent_cuit ON stk_logisticas (enterprise_id, cuit);

-- 📦 TABLA: stk_motivos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_motivos_ent_user_id ON stk_motivos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_motivos_ent_created_at ON stk_motivos (enterprise_id, created_at);

-- 📦 TABLA: stk_movimientos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por motivo_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_ent_motivo_id ON stk_movimientos (enterprise_id, motivo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por deposito_origen_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_ent_deposito_origen_id ON stk_movimientos (enterprise_id, deposito_origen_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por deposito_destino_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_ent_deposito_destino_id ON stk_movimientos (enterprise_id, deposito_destino_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_ent_comprobante_id ON stk_movimientos (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_ent_user_id ON stk_movimientos (enterprise_id, user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_ent_tercero_id ON stk_movimientos (enterprise_id, tercero_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha en el tenant.
CREATE INDEX idx_stk_movimientos_ent_fecha ON stk_movimientos (enterprise_id, fecha);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_movimientos_ent_created_at ON stk_movimientos (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_movimientos_ent_estado ON stk_movimientos (enterprise_id, estado);

-- 📦 TABLA: stk_movimientos_detalle
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por movimiento_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_detalle_ent_movimiento_id ON stk_movimientos_detalle (enterprise_id, movimiento_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_detalle_ent_articulo_id ON stk_movimientos_detalle (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_movimientos_detalle_ent_user_id ON stk_movimientos_detalle (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_movimientos_detalle_ent_created_at ON stk_movimientos_detalle (enterprise_id, created_at);

-- 📦 TABLA: stk_numeros_serie
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_stk_numeros_serie_ent_tercero_id ON stk_numeros_serie (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por ubicacion_id bajo el tenant actual.
CREATE INDEX idx_stk_numeros_serie_ent_ubicacion_id ON stk_numeros_serie (enterprise_id, ubicacion_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_numeros_serie_ent_created_at ON stk_numeros_serie (enterprise_id, created_at);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por numero_serie
CREATE INDEX idx_stk_numeros_serie_ent_numero_serie ON stk_numeros_serie (enterprise_id, numero_serie);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_numeros_serie_ent_estado ON stk_numeros_serie (enterprise_id, estado);

-- 📦 TABLA: stk_pricing_formulas
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_pricing_formulas_ent_created_at ON stk_pricing_formulas (enterprise_id, created_at);

-- 📦 TABLA: stk_pricing_propuestas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por lista_id bajo el tenant actual.
CREATE INDEX idx_stk_pricing_propuestas_ent_lista_id ON stk_pricing_propuestas (enterprise_id, lista_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por documento_origen_id bajo el tenant actual.
CREATE INDEX idx_stk_pricing_propuestas_ent_documento_origen_id ON stk_pricing_propuestas (enterprise_id, documento_origen_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_pricing_propuestas_ent_articulo_id ON stk_pricing_propuestas (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por metodo_costeo_id bajo el tenant actual.
CREATE INDEX idx_stk_pricing_propuestas_ent_metodo_costeo_id ON stk_pricing_propuestas (enterprise_id, metodo_costeo_id);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_pricing_propuestas_ent_estado ON stk_pricing_propuestas (enterprise_id, estado);

-- 📦 TABLA: stk_pricing_reglas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por lista_precio_id bajo el tenant actual.
CREATE INDEX idx_stk_pricing_reglas_ent_lista_precio_id ON stk_pricing_reglas (enterprise_id, lista_precio_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por metodo_costo_id bajo el tenant actual.
CREATE INDEX idx_stk_pricing_reglas_ent_metodo_costo_id ON stk_pricing_reglas (enterprise_id, metodo_costo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por formula_id bajo el tenant actual.
CREATE INDEX idx_stk_pricing_reglas_ent_formula_id ON stk_pricing_reglas (enterprise_id, formula_id);

-- 📦 TABLA: stk_recepciones
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por orden_compra_id bajo el tenant actual.
CREATE INDEX idx_stk_recepciones_ent_orden_compra_id ON stk_recepciones (enterprise_id, orden_compra_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_recepciones_ent_created_at ON stk_recepciones (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_recepciones_ent_estado ON stk_recepciones (enterprise_id, estado);

-- 📦 TABLA: stk_series_counter
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_series_counter_ent_created_at ON stk_series_counter (enterprise_id, created_at);

-- 📦 TABLA: stk_series_trazabilidad
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por serie_id bajo el tenant actual.
CREATE INDEX idx_stk_series_trazabilidad_ent_serie_id ON stk_series_trazabilidad (enterprise_id, serie_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por tercero_id bajo el tenant actual.
CREATE INDEX idx_stk_series_trazabilidad_ent_tercero_id ON stk_series_trazabilidad (enterprise_id, tercero_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por deposito_id bajo el tenant actual.
CREATE INDEX idx_stk_series_trazabilidad_ent_deposito_id ON stk_series_trazabilidad (enterprise_id, deposito_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por comprobante_id bajo el tenant actual.
CREATE INDEX idx_stk_series_trazabilidad_ent_comprobante_id ON stk_series_trazabilidad (enterprise_id, comprobante_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_series_trazabilidad_ent_user_id ON stk_series_trazabilidad (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha en el tenant.
CREATE INDEX idx_stk_series_trazabilidad_ent_fecha ON stk_series_trazabilidad (enterprise_id, fecha);

-- 📦 TABLA: stk_servicios_config
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por articulo_id bajo el tenant actual.
CREATE INDEX idx_stk_servicios_config_ent_articulo_id ON stk_servicios_config (enterprise_id, articulo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_servicios_config_ent_user_id ON stk_servicios_config (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_servicios_config_ent_created_at ON stk_servicios_config (enterprise_id, created_at);

-- 📦 TABLA: stk_tipos_articulo
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_tipos_articulo_ent_user_id ON stk_tipos_articulo (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_tipos_articulo_ent_created_at ON stk_tipos_articulo (enterprise_id, created_at);

-- 📦 TABLA: stk_tipos_articulo_servicios
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por servicio_id bajo el tenant actual.
CREATE INDEX idx_stk_tipos_articulo_servicios_ent_servicio_id ON stk_tipos_articulo_servicios (enterprise_id, servicio_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_tipos_articulo_servicios_ent_user_id ON stk_tipos_articulo_servicios (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_tipos_articulo_servicios_ent_created_at ON stk_tipos_articulo_servicios (enterprise_id, created_at);

-- 📦 TABLA: stk_tipos_articulos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stk_tipos_articulos_ent_user_id ON stk_tipos_articulos (enterprise_id, user_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_tipos_articulos_ent_created_at ON stk_tipos_articulos (enterprise_id, created_at);

-- 📦 TABLA: stk_transferencias
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por origen_id bajo el tenant actual.
CREATE INDEX idx_stk_transferencias_ent_origen_id ON stk_transferencias (enterprise_id, origen_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por destino_id bajo el tenant actual.
CREATE INDEX idx_stk_transferencias_ent_destino_id ON stk_transferencias (enterprise_id, destino_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por logistica_id bajo el tenant actual.
CREATE INDEX idx_stk_transferencias_ent_logistica_id ON stk_transferencias (enterprise_id, logistica_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por usuario_id bajo el tenant actual.
CREATE INDEX idx_stk_transferencias_ent_usuario_id ON stk_transferencias (enterprise_id, usuario_id);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por fecha en el tenant.
CREATE INDEX idx_stk_transferencias_ent_fecha ON stk_transferencias (enterprise_id, fecha);
-- Razón: Optimización de Búsqueda Histórica: Para reportes filtrados por created_at en el tenant.
CREATE INDEX idx_stk_transferencias_ent_created_at ON stk_transferencias (enterprise_id, created_at);
-- Razón: Index Selectivo de Workflow: Optimiza vistas filtradas como 'Ordenes PENDIENTES'
CREATE INDEX idx_stk_transferencias_ent_estado ON stk_transferencias (enterprise_id, estado);

-- 📦 TABLA: stock_ajustes
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por libro_id bajo el tenant actual.
CREATE INDEX idx_stock_ajustes_ent_libro_id ON stock_ajustes (enterprise_id, libro_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por motivo_id bajo el tenant actual.
CREATE INDEX idx_stock_ajustes_ent_motivo_id ON stock_ajustes (enterprise_id, motivo_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stock_ajustes_ent_user_id ON stock_ajustes (enterprise_id, user_id);

-- 📦 TABLA: stock_motivos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_stock_motivos_ent_user_id ON stock_motivos (enterprise_id, user_id);

-- 📦 TABLA: sys_active_tasks
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por task_id bajo el tenant actual.
CREATE INDEX idx_sys_active_tasks_ent_task_id ON sys_active_tasks (enterprise_id, task_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por parent_id bajo el tenant actual.
CREATE INDEX idx_sys_active_tasks_ent_parent_id ON sys_active_tasks (enterprise_id, parent_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por thread_id bajo el tenant actual.
CREATE INDEX idx_sys_active_tasks_ent_thread_id ON sys_active_tasks (enterprise_id, thread_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_active_tasks_ent_user_id ON sys_active_tasks (enterprise_id, user_id);

-- 📦 TABLA: sys_ai_feedback
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_ai_feedback_ent_user_id ON sys_ai_feedback (enterprise_id, user_id);

-- 📦 TABLA: sys_approval_signatures
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por approval_id bajo el tenant actual.
CREATE INDEX idx_sys_approval_signatures_ent_approval_id ON sys_approval_signatures (enterprise_id, approval_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_approval_signatures_ent_user_id ON sys_approval_signatures (enterprise_id, user_id);

-- 📦 TABLA: sys_budget_execution
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por budget_id bajo el tenant actual.
CREATE INDEX idx_sys_budget_execution_ent_budget_id ON sys_budget_execution (enterprise_id, budget_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por transaction_id bajo el tenant actual.
CREATE INDEX idx_sys_budget_execution_ent_transaction_id ON sys_budget_execution (enterprise_id, transaction_id);

-- 📦 TABLA: sys_config_fiscal
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por firma_digital_id bajo el tenant actual.
CREATE INDEX idx_sys_config_fiscal_ent_firma_digital_id ON sys_config_fiscal (enterprise_id, firma_digital_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_config_fiscal_ent_user_id ON sys_config_fiscal (enterprise_id, user_id);

-- 📦 TABLA: sys_cost_centers
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por parent_id bajo el tenant actual.
CREATE INDEX idx_sys_cost_centers_ent_parent_id ON sys_cost_centers (enterprise_id, parent_id);

-- 📦 TABLA: sys_crons
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_crons_ent_user_id ON sys_crons (enterprise_id, user_id);

-- 📦 TABLA: sys_crons_logs
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_crons_logs_user_id ON sys_crons_logs (user_id);

-- 📦 TABLA: sys_departamentos
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_departamentos_user_id ON sys_departamentos (user_id);

-- 📦 TABLA: sys_documentos_adjuntos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por entidad_id bajo el tenant actual.
CREATE INDEX idx_sys_documentos_adjuntos_ent_entidad_id ON sys_documentos_adjuntos (enterprise_id, entidad_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_documentos_adjuntos_ent_user_id ON sys_documentos_adjuntos (enterprise_id, user_id);

-- 📦 TABLA: sys_enrichment_counters
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_enrichment_counters_user_id ON sys_enrichment_counters (user_id);

-- 📦 TABLA: sys_enterprise_logos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_enterprise_logos_ent_user_id ON sys_enterprise_logos (enterprise_id, user_id);

-- 📦 TABLA: sys_enterprise_numeracion
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_enterprise_numeracion_ent_user_id ON sys_enterprise_numeracion (enterprise_id, user_id);

-- 📦 TABLA: sys_enterprises
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_enterprises_user_id ON sys_enterprises (user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por cuit
CREATE INDEX idx_sys_enterprises_cuit ON sys_enterprises (cuit);

-- 📦 TABLA: sys_enterprises_fiscal
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_enterprises_fiscal_ent_user_id ON sys_enterprises_fiscal (enterprise_id, user_id);

-- 📦 TABLA: sys_enterprises_new
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_enterprises_new_user_id ON sys_enterprises_new (user_id);

-- 📦 TABLA: sys_external_services
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_external_services_ent_user_id ON sys_external_services (enterprise_id, user_id);

-- 📦 TABLA: sys_fiscal_comprobante_rules
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_fiscal_comprobante_rules_user_id ON sys_fiscal_comprobante_rules (user_id);

-- 📦 TABLA: sys_impuestos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_impuestos_ent_user_id ON sys_impuestos (enterprise_id, user_id);

-- 📦 TABLA: sys_invoice_layouts
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_invoice_layouts_ent_user_id ON sys_invoice_layouts (enterprise_id, user_id);

-- 📦 TABLA: sys_jurisdicciones
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_jurisdicciones_user_id ON sys_jurisdicciones (user_id);

-- 📦 TABLA: sys_jurisdicciones_iibb
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_jurisdicciones_iibb_user_id ON sys_jurisdicciones_iibb (user_id);

-- 📦 TABLA: sys_localidades
-- Razón: Falta índice FK en municipio_id crítico para JOIN performance.
CREATE INDEX idx_sys_localidades_municipio_id ON sys_localidades (municipio_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_localidades_user_id ON sys_localidades (user_id);

-- 📦 TABLA: sys_municipios
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_municipios_user_id ON sys_municipios (user_id);

-- 📦 TABLA: sys_padrones_iibb
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_padrones_iibb_user_id ON sys_padrones_iibb (user_id);

-- 📦 TABLA: sys_padrones_logs
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_padrones_logs_user_id ON sys_padrones_logs (user_id);

-- 📦 TABLA: sys_permissions
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_permissions_ent_user_id ON sys_permissions (enterprise_id, user_id);

-- 📦 TABLA: sys_provincias
-- Razón: Falta índice FK en iso_id crítico para JOIN performance.
CREATE INDEX idx_sys_provincias_iso_id ON sys_provincias (iso_id);
-- Razón: Falta índice FK en user_id crítico para JOIN performance.
CREATE INDEX idx_sys_provincias_user_id ON sys_provincias (user_id);

-- 📦 TABLA: sys_risk_active_mitigations
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por rule_id bajo el tenant actual.
CREATE INDEX idx_sys_risk_active_mitigations_ent_rule_id ON sys_risk_active_mitigations (enterprise_id, rule_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por target_user_id bajo el tenant actual.
CREATE INDEX idx_sys_risk_active_mitigations_ent_target_user_id ON sys_risk_active_mitigations (enterprise_id, target_user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_risk_active_mitigations_ent_user_id ON sys_risk_active_mitigations (enterprise_id, user_id);

-- 📦 TABLA: sys_risk_mitigation_rules
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_risk_mitigation_rules_ent_user_id ON sys_risk_mitigation_rules (enterprise_id, user_id);

-- 📦 TABLA: sys_role_permissions
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por role_id bajo el tenant actual.
CREATE INDEX idx_sys_role_permissions_ent_role_id ON sys_role_permissions (enterprise_id, role_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por permission_id bajo el tenant actual.
CREATE INDEX idx_sys_role_permissions_ent_permission_id ON sys_role_permissions (enterprise_id, permission_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_role_permissions_ent_user_id ON sys_role_permissions (enterprise_id, user_id);

-- 📦 TABLA: sys_roles
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_roles_ent_user_id ON sys_roles (enterprise_id, user_id);

-- 📦 TABLA: sys_security_logs
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por actor_user_id bajo el tenant actual.
CREATE INDEX idx_sys_security_logs_ent_actor_user_id ON sys_security_logs (enterprise_id, actor_user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por target_user_id bajo el tenant actual.
CREATE INDEX idx_sys_security_logs_ent_target_user_id ON sys_security_logs (enterprise_id, target_user_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por session_id bajo el tenant actual.
CREATE INDEX idx_sys_security_logs_ent_session_id ON sys_security_logs (enterprise_id, session_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_security_logs_ent_user_id ON sys_security_logs (enterprise_id, user_id);

-- 📦 TABLA: sys_tipos_comprobante
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_tipos_comprobante_ent_user_id ON sys_tipos_comprobante (enterprise_id, user_id);

-- 📦 TABLA: sys_transaction_approvals
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por transaction_id bajo el tenant actual.
CREATE INDEX idx_sys_transaction_approvals_ent_transaction_id ON sys_transaction_approvals (enterprise_id, transaction_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por rule_id bajo el tenant actual.
CREATE INDEX idx_sys_transaction_approvals_ent_rule_id ON sys_transaction_approvals (enterprise_id, rule_id);

-- 📦 TABLA: sys_transaction_logs
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_transaction_logs_ent_user_id ON sys_transaction_logs (enterprise_id, user_id);

-- 📦 TABLA: sys_users
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por role_id bajo el tenant actual.
CREATE INDEX idx_sys_users_ent_role_id ON sys_users (enterprise_id, role_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por email
CREATE INDEX idx_sys_users_ent_email ON sys_users (enterprise_id, email);

-- 📦 TABLA: sys_workflow_steps
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por rule_id bajo el tenant actual.
CREATE INDEX idx_sys_workflow_steps_ent_rule_id ON sys_workflow_steps (enterprise_id, rule_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por role_id bajo el tenant actual.
CREATE INDEX idx_sys_workflow_steps_ent_role_id ON sys_workflow_steps (enterprise_id, role_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_sys_workflow_steps_ent_user_id ON sys_workflow_steps (enterprise_id, user_id);

-- 📦 TABLA: system_stats
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_system_stats_ent_user_id ON system_stats (enterprise_id, user_id);

-- 📦 TABLA: tax_alicuotas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_tax_alicuotas_ent_user_id ON tax_alicuotas (enterprise_id, user_id);

-- 📦 TABLA: tax_engine_snapshots
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por version_id bajo el tenant actual.
CREATE INDEX idx_tax_engine_snapshots_ent_version_id ON tax_engine_snapshots (enterprise_id, version_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_tax_engine_snapshots_ent_user_id ON tax_engine_snapshots (enterprise_id, user_id);

-- 📦 TABLA: tax_engine_versions
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por usuario_id bajo el tenant actual.
CREATE INDEX idx_tax_engine_versions_ent_usuario_id ON tax_engine_versions (enterprise_id, usuario_id);

-- 📦 TABLA: tax_impuestos
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_tax_impuestos_ent_user_id ON tax_impuestos (enterprise_id, user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por codigo
CREATE INDEX idx_tax_impuestos_ent_codigo ON tax_impuestos (enterprise_id, codigo);

-- 📦 TABLA: tax_reglas
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por impuesto_id bajo el tenant actual.
CREATE INDEX idx_tax_reglas_ent_impuesto_id ON tax_reglas (enterprise_id, impuesto_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_tax_reglas_ent_user_id ON tax_reglas (enterprise_id, user_id);

-- 📦 TABLA: tax_reglas_iibb
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por impuesto_id bajo el tenant actual.
CREATE INDEX idx_tax_reglas_iibb_ent_impuesto_id ON tax_reglas_iibb (enterprise_id, impuesto_id);
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_tax_reglas_iibb_ent_user_id ON tax_reglas_iibb (enterprise_id, user_id);

-- 📦 TABLA: usuarios
-- Razón: Optimización Multi-Tenant: Evita escaneo total al hacer JOIN por user_id bajo el tenant actual.
CREATE INDEX idx_usuarios_ent_user_id ON usuarios (enterprise_id, user_id);
-- Razón: Búsqueda Selectiva: Optimiza los WHERE exactos tipo buscador por email
CREATE INDEX idx_usuarios_ent_email ON usuarios (enterprise_id, email);
