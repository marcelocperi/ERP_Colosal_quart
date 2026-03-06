
-- Tabla: clientes
CREATE INDEX idx_clientes_ent_user ON clientes (enterprise_id, user_id);
CREATE INDEX idx_clientes_ent_activo ON clientes (enterprise_id, activo);
CREATE INDEX idx_clientes_ent_created_at ON clientes (enterprise_id, created_at);
CREATE INDEX idx_clientes_user ON clientes (user_id);
CREATE INDEX idx_clientes_codigo ON clientes (enterprise_id, codigo);
CREATE INDEX idx_clientes_email ON clientes (enterprise_id, email);
CREATE INDEX idx_clientes_cuit ON clientes (enterprise_id, cuit);

-- Tabla: cmp_articulos_costos_indirectos
CREATE INDEX idx_cmp_articulos_costos_indirectos_ent_user ON cmp_articulos_costos_indirectos (enterprise_id, user_id);
CREATE INDEX idx_cmp_articulos_costos_indirectos_ent_articulo ON cmp_articulos_costos_indirectos (enterprise_id, articulo_id);
CREATE INDEX idx_cmp_articulos_costos_indirectos_ent_activo ON cmp_articulos_costos_indirectos (enterprise_id, activo);
CREATE INDEX idx_cmp_articulos_costos_indirectos_ent_created_at ON cmp_articulos_costos_indirectos (enterprise_id, created_at);
CREATE INDEX idx_cmp_articulos_costos_indirectos_user ON cmp_articulos_costos_indirectos (user_id);

-- Tabla: cmp_articulos_proveedores
CREATE INDEX idx_cmp_articulos_proveedores_ent_origen ON cmp_articulos_proveedores (enterprise_id, origen_id);
CREATE INDEX idx_cmp_articulos_proveedores_ent_user ON cmp_articulos_proveedores (enterprise_id, user_id);
CREATE INDEX idx_cmp_articulos_proveedores_ent_proveedor ON cmp_articulos_proveedores (enterprise_id, proveedor_id);
CREATE INDEX idx_cmp_articulos_proveedores_ent_created_at ON cmp_articulos_proveedores (enterprise_id, created_at);
CREATE INDEX idx_cmp_articulos_proveedores_origen ON cmp_articulos_proveedores (origen_id);
CREATE INDEX idx_cmp_articulos_proveedores_user ON cmp_articulos_proveedores (user_id);

-- Tabla: cmp_consignaciones
CREATE INDEX idx_cmp_consignaciones_ent_tercero ON cmp_consignaciones (enterprise_id, tercero_id);
CREATE INDEX idx_cmp_consignaciones_ent_user ON cmp_consignaciones (enterprise_id, user_id);
CREATE INDEX idx_cmp_consignaciones_ent_deposito ON cmp_consignaciones (enterprise_id, deposito_id);
CREATE INDEX idx_cmp_consignaciones_ent_estado ON cmp_consignaciones (enterprise_id, estado);
CREATE INDEX idx_cmp_consignaciones_ent_created_at ON cmp_consignaciones (enterprise_id, created_at);
CREATE INDEX idx_cmp_consignaciones_user ON cmp_consignaciones (user_id);
CREATE INDEX idx_cmp_consignaciones_deposito ON cmp_consignaciones (deposito_id);

-- Tabla: cmp_cotizaciones
CREATE INDEX idx_cmp_cotizaciones_ent_user ON cmp_cotizaciones (enterprise_id, user_id);
CREATE INDEX idx_cmp_cotizaciones_ent_proveedor ON cmp_cotizaciones (enterprise_id, proveedor_id);
CREATE INDEX idx_cmp_cotizaciones_ent_solicitud_origen ON cmp_cotizaciones (enterprise_id, solicitud_origen_id);
CREATE INDEX idx_cmp_cotizaciones_ent_estado ON cmp_cotizaciones (enterprise_id, estado);
CREATE INDEX idx_cmp_cotizaciones_ent_created_at ON cmp_cotizaciones (enterprise_id, created_at);
CREATE INDEX idx_cmp_cotizaciones_enterprise ON cmp_cotizaciones (enterprise_id);
CREATE INDEX idx_cmp_cotizaciones_user ON cmp_cotizaciones (user_id);

-- Tabla: cmp_detalles_orden
CREATE INDEX idx_cmp_detalles_orden_ent_user ON cmp_detalles_orden (enterprise_id, user_id);
CREATE INDEX idx_cmp_detalles_orden_ent_articulo ON cmp_detalles_orden (enterprise_id, articulo_id);
CREATE INDEX idx_cmp_detalles_orden_ent_orden ON cmp_detalles_orden (enterprise_id, orden_id);
CREATE INDEX idx_cmp_detalles_orden_ent_created_at ON cmp_detalles_orden (enterprise_id, created_at);
CREATE INDEX idx_cmp_detalles_orden_user ON cmp_detalles_orden (user_id);

-- Tabla: cmp_detalles_solicitud
CREATE INDEX idx_cmp_detalles_solicitud_ent_solicitud ON cmp_detalles_solicitud (enterprise_id, solicitud_id);
CREATE INDEX idx_cmp_detalles_solicitud_ent_user ON cmp_detalles_solicitud (enterprise_id, user_id);
CREATE INDEX idx_cmp_detalles_solicitud_ent_articulo ON cmp_detalles_solicitud (enterprise_id, articulo_id);
CREATE INDEX idx_cmp_detalles_solicitud_ent_created_at ON cmp_detalles_solicitud (enterprise_id, created_at);
CREATE INDEX idx_cmp_detalles_solicitud_user ON cmp_detalles_solicitud (user_id);

-- Tabla: cmp_items_consignacion
CREATE INDEX idx_cmp_items_consignacion_moneda ON cmp_items_consignacion (moneda_id);
CREATE INDEX idx_cmp_items_consignacion_user ON cmp_items_consignacion (user_id);

-- Tabla: cmp_items_cotizacion
CREATE INDEX idx_cmp_items_cotizacion_ent_cotizacion ON cmp_items_cotizacion (enterprise_id, cotizacion_id);
CREATE INDEX idx_cmp_items_cotizacion_ent_user ON cmp_items_cotizacion (enterprise_id, user_id);
CREATE INDEX idx_cmp_items_cotizacion_ent_articulo ON cmp_items_cotizacion (enterprise_id, articulo_id);
CREATE INDEX idx_cmp_items_cotizacion_ent_created_at ON cmp_items_cotizacion (enterprise_id, created_at);
CREATE INDEX idx_cmp_items_cotizacion_user ON cmp_items_cotizacion (user_id);

-- Tabla: cmp_liquidaciones_consignacion
CREATE INDEX idx_cmp_liquidaciones_consignacion_comprobante ON cmp_liquidaciones_consignacion (comprobante_id);
CREATE INDEX idx_cmp_liquidaciones_consignacion_user ON cmp_liquidaciones_consignacion (user_id);

-- Tabla: cmp_ordenes_compra
CREATE INDEX idx_cmp_ordenes_compra_ent_proveedor ON cmp_ordenes_compra (enterprise_id, proveedor_id);
CREATE INDEX idx_cmp_ordenes_compra_ent_aprobador_compras ON cmp_ordenes_compra (enterprise_id, aprobador_compras_id);
CREATE INDEX idx_cmp_ordenes_compra_ent_aprobador_tesoreria ON cmp_ordenes_compra (enterprise_id, aprobador_tesoreria_id);
CREATE INDEX idx_cmp_ordenes_compra_ent_centro_costo ON cmp_ordenes_compra (enterprise_id, centro_costo_id);
CREATE INDEX idx_cmp_ordenes_compra_ent_cotizacion ON cmp_ordenes_compra (enterprise_id, cotizacion_id);
CREATE INDEX idx_cmp_ordenes_compra_ent_user ON cmp_ordenes_compra (enterprise_id, user_id);
CREATE INDEX idx_cmp_ordenes_compra_ent_estado ON cmp_ordenes_compra (enterprise_id, estado);
CREATE INDEX idx_cmp_ordenes_compra_ent_created_at ON cmp_ordenes_compra (enterprise_id, created_at);
CREATE INDEX idx_cmp_ordenes_compra_ent_fecha ON cmp_ordenes_compra (enterprise_id, fecha);
CREATE INDEX idx_cmp_ordenes_compra_ent_fecha_emision ON cmp_ordenes_compra (enterprise_id, fecha_emision);
CREATE INDEX idx_cmp_ordenes_compra_aprobador_compras ON cmp_ordenes_compra (aprobador_compras_id);
CREATE INDEX idx_cmp_ordenes_compra_aprobador_tesoreria ON cmp_ordenes_compra (aprobador_tesoreria_id);
CREATE INDEX idx_cmp_ordenes_compra_cotizacion ON cmp_ordenes_compra (cotizacion_id);
CREATE INDEX idx_cmp_ordenes_compra_user ON cmp_ordenes_compra (user_id);

-- Tabla: cmp_overhead_cuenta_contable
CREATE INDEX idx_cmp_overhead_cuenta_contable_ent_activo ON cmp_overhead_cuenta_contable (enterprise_id, activo);

-- Tabla: cmp_overhead_templates
CREATE INDEX idx_cmp_overhead_templates_ent_user ON cmp_overhead_templates (enterprise_id, user_id);
CREATE INDEX idx_cmp_overhead_templates_ent_activo ON cmp_overhead_templates (enterprise_id, activo);
CREATE INDEX idx_cmp_overhead_templates_ent_created_at ON cmp_overhead_templates (enterprise_id, created_at);
CREATE INDEX idx_cmp_overhead_templates_enterprise ON cmp_overhead_templates (enterprise_id);
CREATE INDEX idx_cmp_overhead_templates_user ON cmp_overhead_templates (user_id);

-- Tabla: cmp_overhead_templates_detalle
CREATE INDEX idx_cmp_overhead_templates_detalle_ent_template ON cmp_overhead_templates_detalle (enterprise_id, template_id);
CREATE INDEX idx_cmp_overhead_templates_detalle_ent_user ON cmp_overhead_templates_detalle (enterprise_id, user_id);
CREATE INDEX idx_cmp_overhead_templates_detalle_ent_created_at ON cmp_overhead_templates_detalle (enterprise_id, created_at);
CREATE INDEX idx_cmp_overhead_templates_detalle_enterprise ON cmp_overhead_templates_detalle (enterprise_id);
CREATE INDEX idx_cmp_overhead_templates_detalle_user ON cmp_overhead_templates_detalle (user_id);

-- Tabla: cmp_recetas_bom
CREATE INDEX idx_cmp_recetas_bom_ent_user ON cmp_recetas_bom (enterprise_id, user_id);
CREATE INDEX idx_cmp_recetas_bom_ent_producto ON cmp_recetas_bom (enterprise_id, producto_id);
CREATE INDEX idx_cmp_recetas_bom_ent_activo ON cmp_recetas_bom (enterprise_id, activo);
CREATE INDEX idx_cmp_recetas_bom_ent_created_at ON cmp_recetas_bom (enterprise_id, created_at);
CREATE INDEX idx_cmp_recetas_bom_user ON cmp_recetas_bom (user_id);

-- Tabla: cmp_recetas_detalle
CREATE INDEX idx_cmp_recetas_detalle_user ON cmp_recetas_detalle (user_id);

-- Tabla: cmp_rfq_campanas
CREATE INDEX idx_cmp_rfq_campanas_ent_articulo_objetivo ON cmp_rfq_campanas (enterprise_id, articulo_objetivo_id);
CREATE INDEX idx_cmp_rfq_campanas_ent_user ON cmp_rfq_campanas (enterprise_id, user_id);
CREATE INDEX idx_cmp_rfq_campanas_ent_estado ON cmp_rfq_campanas (enterprise_id, estado);
CREATE INDEX idx_cmp_rfq_campanas_ent_created_at ON cmp_rfq_campanas (enterprise_id, created_at);
CREATE INDEX idx_cmp_rfq_campanas_ent_fecha_emision ON cmp_rfq_campanas (enterprise_id, fecha_emision);
CREATE INDEX idx_cmp_rfq_campanas_articulo_objetivo ON cmp_rfq_campanas (articulo_objetivo_id);
CREATE INDEX idx_cmp_rfq_campanas_user ON cmp_rfq_campanas (user_id);

-- Tabla: cmp_rfq_cotizaciones
CREATE INDEX idx_cmp_rfq_cotizaciones_proveedor ON cmp_rfq_cotizaciones (proveedor_id);

-- Tabla: cmp_rfq_detalles
CREATE INDEX idx_cmp_rfq_detalles_articulo_insumo ON cmp_rfq_detalles (articulo_insumo_id);

-- Tabla: cmp_solicitudes_reposicion
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_centro_costo ON cmp_solicitudes_reposicion (enterprise_id, centro_costo_id);
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_aprobador ON cmp_solicitudes_reposicion (enterprise_id, aprobador_id);
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_user ON cmp_solicitudes_reposicion (enterprise_id, user_id);
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_solicitante ON cmp_solicitudes_reposicion (enterprise_id, solicitante_id);
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_estado ON cmp_solicitudes_reposicion (enterprise_id, estado);
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_fecha ON cmp_solicitudes_reposicion (enterprise_id, fecha);
CREATE INDEX idx_cmp_solicitudes_reposicion_ent_created_at ON cmp_solicitudes_reposicion (enterprise_id, created_at);
CREATE INDEX idx_cmp_solicitudes_reposicion_enterprise ON cmp_solicitudes_reposicion (enterprise_id);
CREATE INDEX idx_cmp_solicitudes_reposicion_aprobador ON cmp_solicitudes_reposicion (aprobador_id);
CREATE INDEX idx_cmp_solicitudes_reposicion_user ON cmp_solicitudes_reposicion (user_id);
CREATE INDEX idx_cmp_solicitudes_reposicion_solicitante ON cmp_solicitudes_reposicion (solicitante_id);

-- Tabla: cmp_sourcing_origenes
CREATE INDEX idx_cmp_sourcing_origenes_ent_user ON cmp_sourcing_origenes (enterprise_id, user_id);
CREATE INDEX idx_cmp_sourcing_origenes_ent_activo ON cmp_sourcing_origenes (enterprise_id, activo);
CREATE INDEX idx_cmp_sourcing_origenes_ent_created_at ON cmp_sourcing_origenes (enterprise_id, created_at);
CREATE INDEX idx_cmp_sourcing_origenes_user ON cmp_sourcing_origenes (user_id);

-- Tabla: cont_asientos
CREATE INDEX idx_cont_asientos_ent_comprobante ON cont_asientos (enterprise_id, comprobante_id);
CREATE INDEX idx_cont_asientos_ent_user ON cont_asientos (enterprise_id, user_id);
CREATE INDEX idx_cont_asientos_ent_estado ON cont_asientos (enterprise_id, estado);
CREATE INDEX idx_cont_asientos_ent_fecha ON cont_asientos (enterprise_id, fecha);
CREATE INDEX idx_cont_asientos_ent_created_at ON cont_asientos (enterprise_id, created_at);
CREATE INDEX idx_cont_asientos_comprobante ON cont_asientos (comprobante_id);
CREATE INDEX idx_cont_asientos_enterprise ON cont_asientos (enterprise_id);
CREATE INDEX idx_cont_asientos_user ON cont_asientos (user_id);

-- Tabla: cont_asientos_detalle
CREATE INDEX idx_cont_asientos_detalle_ent_cuenta ON cont_asientos_detalle (enterprise_id, cuenta_id);
CREATE INDEX idx_cont_asientos_detalle_ent_asiento ON cont_asientos_detalle (enterprise_id, asiento_id);
CREATE INDEX idx_cont_asientos_detalle_ent_user ON cont_asientos_detalle (enterprise_id, user_id);
CREATE INDEX idx_cont_asientos_detalle_ent_created_at ON cont_asientos_detalle (enterprise_id, created_at);
CREATE INDEX idx_cont_asientos_detalle_enterprise ON cont_asientos_detalle (enterprise_id);
CREATE INDEX idx_cont_asientos_detalle_user ON cont_asientos_detalle (user_id);

-- Tabla: cont_plan_cuentas
CREATE INDEX idx_cont_plan_cuentas_ent_padre ON cont_plan_cuentas (enterprise_id, padre_id);
CREATE INDEX idx_cont_plan_cuentas_ent_user ON cont_plan_cuentas (enterprise_id, user_id);
CREATE INDEX idx_cont_plan_cuentas_ent_created_at ON cont_plan_cuentas (enterprise_id, created_at);
CREATE INDEX idx_cont_plan_cuentas_padre ON cont_plan_cuentas (padre_id);
CREATE INDEX idx_cont_plan_cuentas_user ON cont_plan_cuentas (user_id);

-- Tabla: cotizacion_dolar
CREATE INDEX idx_cotizacion_dolar_ent_user ON cotizacion_dolar (enterprise_id, user_id);
CREATE INDEX idx_cotizacion_dolar_ent_created_at ON cotizacion_dolar (enterprise_id, created_at);
CREATE INDEX idx_cotizacion_dolar_user ON cotizacion_dolar (user_id);

-- Tabla: enterprises
CREATE INDEX idx_enterprises_user ON enterprises (user_id);
CREATE INDEX idx_enterprises_cuit ON enterprises (cuit);

-- Tabla: erp_areas
CREATE INDEX idx_erp_areas_ent_activo ON erp_areas (enterprise_id, activo);
CREATE INDEX idx_erp_areas_ent_created_at ON erp_areas (enterprise_id, created_at);

-- Tabla: erp_comprobantes
CREATE INDEX idx_erp_comprobantes_ent_direccion_entrega ON erp_comprobantes (enterprise_id, direccion_entrega_id);
CREATE INDEX idx_erp_comprobantes_ent_tercero ON erp_comprobantes (enterprise_id, tercero_id);
CREATE INDEX idx_erp_comprobantes_ent_receptor_contacto ON erp_comprobantes (enterprise_id, receptor_contacto_id);
CREATE INDEX idx_erp_comprobantes_ent_condicion_pago ON erp_comprobantes (enterprise_id, condicion_pago_id);
CREATE INDEX idx_erp_comprobantes_ent_jurisdiccion ON erp_comprobantes (enterprise_id, jurisdiccion_id);
CREATE INDEX idx_erp_comprobantes_ent_logistica ON erp_comprobantes (enterprise_id, logistica_id);
CREATE INDEX idx_erp_comprobantes_ent_comprobante_asociado ON erp_comprobantes (enterprise_id, comprobante_asociado_id);
CREATE INDEX idx_erp_comprobantes_ent_asiento ON erp_comprobantes (enterprise_id, asiento_id);
CREATE INDEX idx_erp_comprobantes_ent_user ON erp_comprobantes (enterprise_id, user_id);
CREATE INDEX idx_erp_comprobantes_ent_created_at ON erp_comprobantes (enterprise_id, created_at);
CREATE INDEX idx_erp_comprobantes_ent_fecha_emision ON erp_comprobantes (enterprise_id, fecha_emision);
CREATE INDEX idx_erp_comprobantes_direccion_entrega ON erp_comprobantes (direccion_entrega_id);
CREATE INDEX idx_erp_comprobantes_receptor_contacto ON erp_comprobantes (receptor_contacto_id);
CREATE INDEX idx_erp_comprobantes_condicion_pago ON erp_comprobantes (condicion_pago_id);
CREATE INDEX idx_erp_comprobantes_logistica ON erp_comprobantes (logistica_id);
CREATE INDEX idx_erp_comprobantes_enterprise ON erp_comprobantes (enterprise_id);
CREATE INDEX idx_erp_comprobantes_comprobante_asociado ON erp_comprobantes (comprobante_asociado_id);
CREATE INDEX idx_erp_comprobantes_asiento ON erp_comprobantes (asiento_id);
CREATE INDEX idx_erp_comprobantes_user ON erp_comprobantes (user_id);
CREATE INDEX idx_erp_comprobantes_numero ON erp_comprobantes (enterprise_id, numero);

-- Tabla: erp_comprobantes_detalle
CREATE INDEX idx_erp_comprobantes_detalle_ent_comprobante ON erp_comprobantes_detalle (enterprise_id, comprobante_id);
CREATE INDEX idx_erp_comprobantes_detalle_ent_user ON erp_comprobantes_detalle (enterprise_id, user_id);
CREATE INDEX idx_erp_comprobantes_detalle_ent_articulo ON erp_comprobantes_detalle (enterprise_id, articulo_id);
CREATE INDEX idx_erp_comprobantes_detalle_ent_created_at ON erp_comprobantes_detalle (enterprise_id, created_at);
CREATE INDEX idx_erp_comprobantes_detalle_enterprise ON erp_comprobantes_detalle (enterprise_id);
CREATE INDEX idx_erp_comprobantes_detalle_user ON erp_comprobantes_detalle (user_id);
CREATE INDEX idx_erp_comprobantes_detalle_articulo ON erp_comprobantes_detalle (articulo_id);

-- Tabla: erp_comprobantes_impuestos
CREATE INDEX idx_erp_comprobantes_impuestos_ent_jurisdiccion ON erp_comprobantes_impuestos (enterprise_id, jurisdiccion_id);
CREATE INDEX idx_erp_comprobantes_impuestos_ent_comprobante ON erp_comprobantes_impuestos (enterprise_id, comprobante_id);
CREATE INDEX idx_erp_comprobantes_impuestos_ent_user ON erp_comprobantes_impuestos (enterprise_id, user_id);
CREATE INDEX idx_erp_comprobantes_impuestos_ent_impuesto ON erp_comprobantes_impuestos (enterprise_id, impuesto_id);
CREATE INDEX idx_erp_comprobantes_impuestos_ent_created_at ON erp_comprobantes_impuestos (enterprise_id, created_at);
CREATE INDEX idx_erp_comprobantes_impuestos_jurisdiccion ON erp_comprobantes_impuestos (jurisdiccion_id);
CREATE INDEX idx_erp_comprobantes_impuestos_user ON erp_comprobantes_impuestos (user_id);
CREATE INDEX idx_erp_comprobantes_impuestos_impuesto ON erp_comprobantes_impuestos (impuesto_id);

-- Tabla: erp_contactos
CREATE INDEX idx_erp_contactos_ent_tercero ON erp_contactos (enterprise_id, tercero_id);
CREATE INDEX idx_erp_contactos_ent_puesto ON erp_contactos (enterprise_id, puesto_id);
CREATE INDEX idx_erp_contactos_ent_direccion ON erp_contactos (enterprise_id, direccion_id);
CREATE INDEX idx_erp_contactos_ent_user ON erp_contactos (enterprise_id, user_id);
CREATE INDEX idx_erp_contactos_ent_created_at ON erp_contactos (enterprise_id, created_at);
CREATE INDEX idx_erp_contactos_enterprise ON erp_contactos (enterprise_id);
CREATE INDEX idx_erp_contactos_user ON erp_contactos (user_id);
CREATE INDEX idx_erp_contactos_email ON erp_contactos (enterprise_id, email);

-- Tabla: erp_cuentas_fondos
CREATE INDEX idx_erp_cuentas_fondos_ent_user ON erp_cuentas_fondos (enterprise_id, user_id);
CREATE INDEX idx_erp_cuentas_fondos_ent_cuenta_contable ON erp_cuentas_fondos (enterprise_id, cuenta_contable_id);
CREATE INDEX idx_erp_cuentas_fondos_ent_activo ON erp_cuentas_fondos (enterprise_id, activo);
CREATE INDEX idx_erp_cuentas_fondos_ent_created_at ON erp_cuentas_fondos (enterprise_id, created_at);
CREATE INDEX idx_erp_cuentas_fondos_enterprise ON erp_cuentas_fondos (enterprise_id);
CREATE INDEX idx_erp_cuentas_fondos_user ON erp_cuentas_fondos (user_id);
CREATE INDEX idx_erp_cuentas_fondos_cuenta_contable ON erp_cuentas_fondos (cuenta_contable_id);

-- Tabla: erp_datos_fiscales
CREATE INDEX idx_erp_datos_fiscales_ent_tercero ON erp_datos_fiscales (enterprise_id, tercero_id);
CREATE INDEX idx_erp_datos_fiscales_ent_user ON erp_datos_fiscales (enterprise_id, user_id);
CREATE INDEX idx_erp_datos_fiscales_ent_created_at ON erp_datos_fiscales (enterprise_id, created_at);
CREATE INDEX idx_erp_datos_fiscales_enterprise ON erp_datos_fiscales (enterprise_id);
CREATE INDEX idx_erp_datos_fiscales_user ON erp_datos_fiscales (user_id);

-- Tabla: erp_direcciones
CREATE INDEX idx_erp_direcciones_ent_tercero ON erp_direcciones (enterprise_id, tercero_id);
CREATE INDEX idx_erp_direcciones_ent_user ON erp_direcciones (enterprise_id, user_id);
CREATE INDEX idx_erp_direcciones_ent_created_at ON erp_direcciones (enterprise_id, created_at);
CREATE INDEX idx_erp_direcciones_enterprise ON erp_direcciones (enterprise_id);
CREATE INDEX idx_erp_direcciones_user ON erp_direcciones (user_id);
CREATE INDEX idx_erp_direcciones_numero ON erp_direcciones (enterprise_id, numero);

-- Tabla: erp_movimientos_fondos
CREATE INDEX idx_erp_movimientos_fondos_ent_tercero ON erp_movimientos_fondos (enterprise_id, tercero_id);
CREATE INDEX idx_erp_movimientos_fondos_ent_comprobante_asociado ON erp_movimientos_fondos (enterprise_id, comprobante_asociado_id);
CREATE INDEX idx_erp_movimientos_fondos_ent_asiento ON erp_movimientos_fondos (enterprise_id, asiento_id);
CREATE INDEX idx_erp_movimientos_fondos_ent_user ON erp_movimientos_fondos (enterprise_id, user_id);
CREATE INDEX idx_erp_movimientos_fondos_ent_cuenta_fondo ON erp_movimientos_fondos (enterprise_id, cuenta_fondo_id);
CREATE INDEX idx_erp_movimientos_fondos_ent_fecha ON erp_movimientos_fondos (enterprise_id, fecha);
CREATE INDEX idx_erp_movimientos_fondos_ent_created_at ON erp_movimientos_fondos (enterprise_id, created_at);
CREATE INDEX idx_erp_movimientos_fondos_tercero ON erp_movimientos_fondos (tercero_id);
CREATE INDEX idx_erp_movimientos_fondos_enterprise ON erp_movimientos_fondos (enterprise_id);
CREATE INDEX idx_erp_movimientos_fondos_comprobante_asociado ON erp_movimientos_fondos (comprobante_asociado_id);
CREATE INDEX idx_erp_movimientos_fondos_asiento ON erp_movimientos_fondos (asiento_id);
CREATE INDEX idx_erp_movimientos_fondos_user ON erp_movimientos_fondos (user_id);

-- Tabla: erp_puestos
CREATE INDEX idx_erp_puestos_ent_user ON erp_puestos (enterprise_id, user_id);
CREATE INDEX idx_erp_puestos_ent_activo ON erp_puestos (enterprise_id, activo);
CREATE INDEX idx_erp_puestos_ent_created_at ON erp_puestos (enterprise_id, created_at);
CREATE INDEX idx_erp_puestos_user ON erp_puestos (user_id);

-- Tabla: erp_terceros
CREATE INDEX idx_erp_terceros_ent_condicion_pago_pendiente ON erp_terceros (enterprise_id, condicion_pago_pendiente_id);
CREATE INDEX idx_erp_terceros_ent_condicion_pago ON erp_terceros (enterprise_id, condicion_pago_id);
CREATE INDEX idx_erp_terceros_ent_user ON erp_terceros (enterprise_id, user_id);
CREATE INDEX idx_erp_terceros_ent_condicion_mixta ON erp_terceros (enterprise_id, condicion_mixta_id);
CREATE INDEX idx_erp_terceros_ent_activo ON erp_terceros (enterprise_id, activo);
CREATE INDEX idx_erp_terceros_ent_created_at ON erp_terceros (enterprise_id, created_at);
CREATE INDEX idx_erp_terceros_condicion_pago_pendiente ON erp_terceros (condicion_pago_pendiente_id);
CREATE INDEX idx_erp_terceros_condicion_pago ON erp_terceros (condicion_pago_id);
CREATE INDEX idx_erp_terceros_user ON erp_terceros (user_id);
CREATE INDEX idx_erp_terceros_codigo ON erp_terceros (enterprise_id, codigo);
CREATE INDEX idx_erp_terceros_email ON erp_terceros (enterprise_id, email);

-- Tabla: erp_terceros_cm05
CREATE INDEX idx_erp_terceros_cm05_ent_user ON erp_terceros_cm05 (enterprise_id, user_id);
CREATE INDEX idx_erp_terceros_cm05_ent_created_at ON erp_terceros_cm05 (enterprise_id, created_at);
CREATE INDEX idx_erp_terceros_cm05_user ON erp_terceros_cm05 (user_id);

-- Tabla: erp_terceros_condiciones
CREATE INDEX idx_erp_terceros_condiciones_ent_user ON erp_terceros_condiciones (enterprise_id, user_id);
CREATE INDEX idx_erp_terceros_condiciones_ent_condicion_pago ON erp_terceros_condiciones (enterprise_id, condicion_pago_id);
CREATE INDEX idx_erp_terceros_condiciones_ent_created_at ON erp_terceros_condiciones (enterprise_id, created_at);
CREATE INDEX idx_erp_terceros_condiciones_user ON erp_terceros_condiciones (user_id);

-- Tabla: erp_terceros_jurisdicciones
CREATE INDEX idx_erp_terceros_jurisdicciones_ent_user ON erp_terceros_jurisdicciones (enterprise_id, user_id);
CREATE INDEX idx_erp_terceros_jurisdicciones_ent_created_at ON erp_terceros_jurisdicciones (enterprise_id, created_at);
CREATE INDEX idx_erp_terceros_jurisdicciones_user ON erp_terceros_jurisdicciones (user_id);

-- Tabla: fin_bancos
CREATE INDEX idx_fin_bancos_ent_cuenta_contable ON fin_bancos (enterprise_id, cuenta_contable_id);
CREATE INDEX idx_fin_bancos_ent_bcra ON fin_bancos (enterprise_id, bcra_id);
CREATE INDEX idx_fin_bancos_ent_user ON fin_bancos (enterprise_id, user_id);
CREATE INDEX idx_fin_bancos_ent_activo ON fin_bancos (enterprise_id, activo);
CREATE INDEX idx_fin_bancos_ent_created_at ON fin_bancos (enterprise_id, created_at);
CREATE INDEX idx_fin_bancos_cuenta_contable ON fin_bancos (cuenta_contable_id);
CREATE INDEX idx_fin_bancos_user ON fin_bancos (user_id);
CREATE INDEX idx_fin_bancos_cuit ON fin_bancos (enterprise_id, cuit);

-- Tabla: fin_cheques
CREATE INDEX idx_fin_cheques_ent_tercero ON fin_cheques (enterprise_id, tercero_id);
CREATE INDEX idx_fin_cheques_ent_orden_pago_destino ON fin_cheques (enterprise_id, orden_pago_destino_id);
CREATE INDEX idx_fin_cheques_ent_banco ON fin_cheques (enterprise_id, banco_id);
CREATE INDEX idx_fin_cheques_ent_recibo_origen ON fin_cheques (enterprise_id, recibo_origen_id);
CREATE INDEX idx_fin_cheques_ent_user ON fin_cheques (enterprise_id, user_id);
CREATE INDEX idx_fin_cheques_ent_estado ON fin_cheques (enterprise_id, estado);
CREATE INDEX idx_fin_cheques_ent_created_at ON fin_cheques (enterprise_id, created_at);
CREATE INDEX idx_fin_cheques_ent_fecha_emision ON fin_cheques (enterprise_id, fecha_emision);
CREATE INDEX idx_fin_cheques_tercero ON fin_cheques (tercero_id);
CREATE INDEX idx_fin_cheques_orden_pago_destino ON fin_cheques (orden_pago_destino_id);
CREATE INDEX idx_fin_cheques_banco ON fin_cheques (banco_id);
CREATE INDEX idx_fin_cheques_recibo_origen ON fin_cheques (recibo_origen_id);
CREATE INDEX idx_fin_cheques_user ON fin_cheques (user_id);

-- Tabla: fin_condiciones_pago
CREATE INDEX idx_fin_condiciones_pago_ent_user ON fin_condiciones_pago (enterprise_id, user_id);
CREATE INDEX idx_fin_condiciones_pago_ent_activo ON fin_condiciones_pago (enterprise_id, activo);
CREATE INDEX idx_fin_condiciones_pago_ent_created_at ON fin_condiciones_pago (enterprise_id, created_at);
CREATE INDEX idx_fin_condiciones_pago_user ON fin_condiciones_pago (user_id);

-- Tabla: fin_condiciones_pago_mixtas
CREATE INDEX idx_fin_condiciones_pago_mixtas_ent_user ON fin_condiciones_pago_mixtas (enterprise_id, user_id);
CREATE INDEX idx_fin_condiciones_pago_mixtas_ent_activo ON fin_condiciones_pago_mixtas (enterprise_id, activo);
CREATE INDEX idx_fin_condiciones_pago_mixtas_ent_created_at ON fin_condiciones_pago_mixtas (enterprise_id, created_at);
CREATE INDEX idx_fin_condiciones_pago_mixtas_enterprise ON fin_condiciones_pago_mixtas (enterprise_id);
CREATE INDEX idx_fin_condiciones_pago_mixtas_user ON fin_condiciones_pago_mixtas (user_id);

-- Tabla: fin_condiciones_pago_mixtas_detalle
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_ent_user ON fin_condiciones_pago_mixtas_detalle (enterprise_id, user_id);
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_ent_mixta ON fin_condiciones_pago_mixtas_detalle (enterprise_id, mixta_id);
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_ent_condicion_pago ON fin_condiciones_pago_mixtas_detalle (enterprise_id, condicion_pago_id);
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_ent_created_at ON fin_condiciones_pago_mixtas_detalle (enterprise_id, created_at);
CREATE INDEX idx_fin_condiciones_pago_mixtas_detalle_user ON fin_condiciones_pago_mixtas_detalle (user_id);

-- Tabla: fin_devoluciones_valores
CREATE INDEX idx_fin_devoluciones_valores_medio_pago ON fin_devoluciones_valores (medio_pago_id);
CREATE INDEX idx_fin_devoluciones_valores_user ON fin_devoluciones_valores (user_id);

-- Tabla: fin_factura_cobros
CREATE INDEX idx_fin_factura_cobros_ent_medio_pago ON fin_factura_cobros (enterprise_id, medio_pago_id);
CREATE INDEX idx_fin_factura_cobros_ent_factura ON fin_factura_cobros (enterprise_id, factura_id);
CREATE INDEX idx_fin_factura_cobros_ent_user ON fin_factura_cobros (enterprise_id, user_id);
CREATE INDEX idx_fin_factura_cobros_ent_cuenta_contable_snapshot ON fin_factura_cobros (enterprise_id, cuenta_contable_snapshot_id);
CREATE INDEX idx_fin_factura_cobros_ent_fecha ON fin_factura_cobros (enterprise_id, fecha);
CREATE INDEX idx_fin_factura_cobros_ent_created_at ON fin_factura_cobros (enterprise_id, created_at);
CREATE INDEX idx_fin_factura_cobros_medio_pago ON fin_factura_cobros (medio_pago_id);
CREATE INDEX idx_fin_factura_cobros_user ON fin_factura_cobros (user_id);
CREATE INDEX idx_fin_factura_cobros_cuenta_contable_snapshot ON fin_factura_cobros (cuenta_contable_snapshot_id);

-- Tabla: fin_liquidaciones
CREATE INDEX idx_fin_liquidaciones_ent_nomina ON fin_liquidaciones (enterprise_id, nomina_id);
CREATE INDEX idx_fin_liquidaciones_ent_usuario ON fin_liquidaciones (enterprise_id, usuario_id);
CREATE INDEX idx_fin_liquidaciones_ent_created_at ON fin_liquidaciones (enterprise_id, created_at);
CREATE INDEX idx_fin_liquidaciones_usuario ON fin_liquidaciones (usuario_id);

-- Tabla: fin_medios_pago
CREATE INDEX idx_fin_medios_pago_ent_user ON fin_medios_pago (enterprise_id, user_id);
CREATE INDEX idx_fin_medios_pago_ent_cuenta_contable ON fin_medios_pago (enterprise_id, cuenta_contable_id);
CREATE INDEX idx_fin_medios_pago_ent_activo ON fin_medios_pago (enterprise_id, activo);
CREATE INDEX idx_fin_medios_pago_ent_created_at ON fin_medios_pago (enterprise_id, created_at);
CREATE INDEX idx_fin_medios_pago_user ON fin_medios_pago (user_id);
CREATE INDEX idx_fin_medios_pago_cuenta_contable ON fin_medios_pago (cuenta_contable_id);

-- Tabla: fin_nominas
CREATE INDEX idx_fin_nominas_ent_asiento ON fin_nominas (enterprise_id, asiento_id);
CREATE INDEX idx_fin_nominas_ent_user ON fin_nominas (enterprise_id, user_id);
CREATE INDEX idx_fin_nominas_ent_estado ON fin_nominas (enterprise_id, estado);
CREATE INDEX idx_fin_nominas_ent_created_at ON fin_nominas (enterprise_id, created_at);
CREATE INDEX idx_fin_nominas_enterprise ON fin_nominas (enterprise_id);
CREATE INDEX idx_fin_nominas_asiento ON fin_nominas (asiento_id);
CREATE INDEX idx_fin_nominas_user ON fin_nominas (user_id);

-- Tabla: fin_ordenes_pago
CREATE INDEX idx_fin_ordenes_pago_ent_tercero ON fin_ordenes_pago (enterprise_id, tercero_id);
CREATE INDEX idx_fin_ordenes_pago_ent_user ON fin_ordenes_pago (enterprise_id, user_id);
CREATE INDEX idx_fin_ordenes_pago_ent_estado ON fin_ordenes_pago (enterprise_id, estado);
CREATE INDEX idx_fin_ordenes_pago_ent_fecha ON fin_ordenes_pago (enterprise_id, fecha);
CREATE INDEX idx_fin_ordenes_pago_ent_created_at ON fin_ordenes_pago (enterprise_id, created_at);
CREATE INDEX idx_fin_ordenes_pago_tercero ON fin_ordenes_pago (tercero_id);
CREATE INDEX idx_fin_ordenes_pago_enterprise ON fin_ordenes_pago (enterprise_id);
CREATE INDEX idx_fin_ordenes_pago_user ON fin_ordenes_pago (user_id);
CREATE INDEX idx_fin_ordenes_pago_numero ON fin_ordenes_pago (enterprise_id, numero);

-- Tabla: fin_ordenes_pago_comprobantes
CREATE INDEX idx_fin_ordenes_pago_comprobantes_ent_orden_pago ON fin_ordenes_pago_comprobantes (enterprise_id, orden_pago_id);
CREATE INDEX idx_fin_ordenes_pago_comprobantes_ent_comprobante ON fin_ordenes_pago_comprobantes (enterprise_id, comprobante_id);
CREATE INDEX idx_fin_ordenes_pago_comprobantes_ent_user ON fin_ordenes_pago_comprobantes (enterprise_id, user_id);
CREATE INDEX idx_fin_ordenes_pago_comprobantes_ent_created_at ON fin_ordenes_pago_comprobantes (enterprise_id, created_at);
CREATE INDEX idx_fin_ordenes_pago_comprobantes_orden_pago ON fin_ordenes_pago_comprobantes (orden_pago_id);
CREATE INDEX idx_fin_ordenes_pago_comprobantes_comprobante ON fin_ordenes_pago_comprobantes (comprobante_id);
CREATE INDEX idx_fin_ordenes_pago_comprobantes_user ON fin_ordenes_pago_comprobantes (user_id);

-- Tabla: fin_ordenes_pago_medios
CREATE INDEX idx_fin_ordenes_pago_medios_ent_orden_pago ON fin_ordenes_pago_medios (enterprise_id, orden_pago_id);
CREATE INDEX idx_fin_ordenes_pago_medios_ent_medio_pago ON fin_ordenes_pago_medios (enterprise_id, medio_pago_id);
CREATE INDEX idx_fin_ordenes_pago_medios_ent_debin ON fin_ordenes_pago_medios (enterprise_id, debin_id);
CREATE INDEX idx_fin_ordenes_pago_medios_ent_banco ON fin_ordenes_pago_medios (enterprise_id, banco_id);
CREATE INDEX idx_fin_ordenes_pago_medios_ent_user ON fin_ordenes_pago_medios (enterprise_id, user_id);
CREATE INDEX idx_fin_ordenes_pago_medios_ent_cuenta_contable_snapshot ON fin_ordenes_pago_medios (enterprise_id, cuenta_contable_snapshot_id);
CREATE INDEX idx_fin_ordenes_pago_medios_ent_created_at ON fin_ordenes_pago_medios (enterprise_id, created_at);
CREATE INDEX idx_fin_ordenes_pago_medios_orden_pago ON fin_ordenes_pago_medios (orden_pago_id);
CREATE INDEX idx_fin_ordenes_pago_medios_medio_pago ON fin_ordenes_pago_medios (medio_pago_id);
CREATE INDEX idx_fin_ordenes_pago_medios_debin ON fin_ordenes_pago_medios (debin_id);
CREATE INDEX idx_fin_ordenes_pago_medios_banco ON fin_ordenes_pago_medios (banco_id);
CREATE INDEX idx_fin_ordenes_pago_medios_user ON fin_ordenes_pago_medios (user_id);
CREATE INDEX idx_fin_ordenes_pago_medios_cuenta_contable_snapshot ON fin_ordenes_pago_medios (cuenta_contable_snapshot_id);

-- Tabla: fin_recibos
CREATE INDEX idx_fin_recibos_ent_tercero ON fin_recibos (enterprise_id, tercero_id);
CREATE INDEX idx_fin_recibos_ent_asiento ON fin_recibos (enterprise_id, asiento_id);
CREATE INDEX idx_fin_recibos_ent_user ON fin_recibos (enterprise_id, user_id);
CREATE INDEX idx_fin_recibos_ent_estado ON fin_recibos (enterprise_id, estado);
CREATE INDEX idx_fin_recibos_ent_fecha ON fin_recibos (enterprise_id, fecha);
CREATE INDEX idx_fin_recibos_ent_created_at ON fin_recibos (enterprise_id, created_at);
CREATE INDEX idx_fin_recibos_asiento ON fin_recibos (asiento_id);
CREATE INDEX idx_fin_recibos_user ON fin_recibos (user_id);
CREATE INDEX idx_fin_recibos_numero ON fin_recibos (enterprise_id, numero);

-- Tabla: fin_recibos_detalles
CREATE INDEX idx_fin_recibos_detalles_user ON fin_recibos_detalles (user_id);

-- Tabla: fin_recibos_medios
CREATE INDEX idx_fin_recibos_medios_medio_pago ON fin_recibos_medios (medio_pago_id);
CREATE INDEX idx_fin_recibos_medios_banco ON fin_recibos_medios (banco_id);
CREATE INDEX idx_fin_recibos_medios_debin ON fin_recibos_medios (debin_id);
CREATE INDEX idx_fin_recibos_medios_user ON fin_recibos_medios (user_id);
CREATE INDEX idx_fin_recibos_medios_cuenta_contable_snapshot ON fin_recibos_medios (cuenta_contable_snapshot_id);

-- Tabla: fin_retenciones_emitidas
CREATE INDEX idx_fin_retenciones_emitidas_ent_tercero ON fin_retenciones_emitidas (enterprise_id, tercero_id);
CREATE INDEX idx_fin_retenciones_emitidas_ent_jurisdiccion ON fin_retenciones_emitidas (enterprise_id, jurisdiccion_id);
CREATE INDEX idx_fin_retenciones_emitidas_ent_user ON fin_retenciones_emitidas (enterprise_id, user_id);
CREATE INDEX idx_fin_retenciones_emitidas_ent_comprobante_pago ON fin_retenciones_emitidas (enterprise_id, comprobante_pago_id);
CREATE INDEX idx_fin_retenciones_emitidas_ent_fecha ON fin_retenciones_emitidas (enterprise_id, fecha);
CREATE INDEX idx_fin_retenciones_emitidas_ent_created_at ON fin_retenciones_emitidas (enterprise_id, created_at);
CREATE INDEX idx_fin_retenciones_emitidas_tercero ON fin_retenciones_emitidas (tercero_id);
CREATE INDEX idx_fin_retenciones_emitidas_jurisdiccion ON fin_retenciones_emitidas (jurisdiccion_id);
CREATE INDEX idx_fin_retenciones_emitidas_enterprise ON fin_retenciones_emitidas (enterprise_id);
CREATE INDEX idx_fin_retenciones_emitidas_user ON fin_retenciones_emitidas (user_id);
CREATE INDEX idx_fin_retenciones_emitidas_comprobante_pago ON fin_retenciones_emitidas (comprobante_pago_id);

-- Tabla: fin_tipos_cambio
CREATE INDEX idx_fin_tipos_cambio_ent_user ON fin_tipos_cambio (enterprise_id, user_id);
CREATE INDEX idx_fin_tipos_cambio_ent_created_at ON fin_tipos_cambio (enterprise_id, created_at);
CREATE INDEX idx_fin_tipos_cambio_user ON fin_tipos_cambio (user_id);

-- Tabla: historial_prestamos
CREATE INDEX idx_historial_prestamos_ent_libro ON historial_prestamos (enterprise_id, libro_id);
CREATE INDEX idx_historial_prestamos_ent_usuario ON historial_prestamos (enterprise_id, usuario_id);
CREATE INDEX idx_historial_prestamos_ent_created_at ON historial_prestamos (enterprise_id, created_at);
CREATE INDEX idx_historial_prestamos_libro ON historial_prestamos (libro_id);
CREATE INDEX idx_historial_prestamos_usuario ON historial_prestamos (usuario_id);

-- Tabla: imp_cargos
CREATE INDEX idx_imp_cargos_ent_despacho ON imp_cargos (enterprise_id, despacho_id);
CREATE INDEX idx_imp_cargos_ent_proveedor ON imp_cargos (enterprise_id, proveedor_id);
CREATE INDEX idx_imp_cargos_ent_comprobante ON imp_cargos (enterprise_id, comprobante_id);
CREATE INDEX idx_imp_cargos_ent_pago ON imp_cargos (enterprise_id, pago_id);
CREATE INDEX idx_imp_cargos_ent_user ON imp_cargos (enterprise_id, user_id);
CREATE INDEX idx_imp_cargos_ent_cargo_referencia ON imp_cargos (enterprise_id, cargo_referencia_id);
CREATE INDEX idx_imp_cargos_ent_orden_compra ON imp_cargos (enterprise_id, orden_compra_id);
CREATE INDEX idx_imp_cargos_ent_estado ON imp_cargos (enterprise_id, estado);
CREATE INDEX idx_imp_cargos_ent_created_at ON imp_cargos (enterprise_id, created_at);
CREATE INDEX idx_imp_cargos_ent_fecha ON imp_cargos (enterprise_id, fecha);
CREATE INDEX idx_imp_cargos_despacho ON imp_cargos (despacho_id);
CREATE INDEX idx_imp_cargos_proveedor ON imp_cargos (proveedor_id);
CREATE INDEX idx_imp_cargos_comprobante ON imp_cargos (comprobante_id);
CREATE INDEX idx_imp_cargos_pago ON imp_cargos (pago_id);
CREATE INDEX idx_imp_cargos_enterprise ON imp_cargos (enterprise_id);
CREATE INDEX idx_imp_cargos_user ON imp_cargos (user_id);
CREATE INDEX idx_imp_cargos_cargo_referencia ON imp_cargos (cargo_referencia_id);

-- Tabla: imp_despachos
CREATE INDEX idx_imp_despachos_ent_despachante ON imp_despachos (enterprise_id, despachante_id);
CREATE INDEX idx_imp_despachos_ent_user ON imp_despachos (enterprise_id, user_id);
CREATE INDEX idx_imp_despachos_ent_orden_compra ON imp_despachos (enterprise_id, orden_compra_id);
CREATE INDEX idx_imp_despachos_ent_estado ON imp_despachos (enterprise_id, estado);
CREATE INDEX idx_imp_despachos_ent_created_at ON imp_despachos (enterprise_id, created_at);
CREATE INDEX idx_imp_despachos_despachante ON imp_despachos (despachante_id);
CREATE INDEX idx_imp_despachos_enterprise ON imp_despachos (enterprise_id);
CREATE INDEX idx_imp_despachos_user ON imp_despachos (user_id);

-- Tabla: imp_despachos_items
CREATE INDEX idx_imp_despachos_items_user ON imp_despachos_items (user_id);
CREATE INDEX idx_imp_despachos_items_orden_compra ON imp_despachos_items (orden_compra_id);

-- Tabla: imp_documentos
CREATE INDEX idx_imp_documentos_ent_despacho ON imp_documentos (enterprise_id, despacho_id);
CREATE INDEX idx_imp_documentos_ent_user ON imp_documentos (enterprise_id, user_id);
CREATE INDEX idx_imp_documentos_ent_orden_compra ON imp_documentos (enterprise_id, orden_compra_id);
CREATE INDEX idx_imp_documentos_ent_proveedor ON imp_documentos (enterprise_id, proveedor_id);
CREATE INDEX idx_imp_documentos_ent_estado ON imp_documentos (enterprise_id, estado);
CREATE INDEX idx_imp_documentos_ent_created_at ON imp_documentos (enterprise_id, created_at);
CREATE INDEX idx_imp_documentos_despacho ON imp_documentos (despacho_id);
CREATE INDEX idx_imp_documentos_enterprise ON imp_documentos (enterprise_id);
CREATE INDEX idx_imp_documentos_user ON imp_documentos (user_id);
CREATE INDEX idx_imp_documentos_proveedor ON imp_documentos (proveedor_id);

-- Tabla: imp_pagos
CREATE INDEX idx_imp_pagos_ent_proveedor ON imp_pagos (enterprise_id, proveedor_id);
CREATE INDEX idx_imp_pagos_ent_banco ON imp_pagos (enterprise_id, banco_id);
CREATE INDEX idx_imp_pagos_ent_asiento ON imp_pagos (enterprise_id, asiento_id);
CREATE INDEX idx_imp_pagos_ent_user ON imp_pagos (enterprise_id, user_id);
CREATE INDEX idx_imp_pagos_ent_orden_compra ON imp_pagos (enterprise_id, orden_compra_id);
CREATE INDEX idx_imp_pagos_ent_estado ON imp_pagos (enterprise_id, estado);
CREATE INDEX idx_imp_pagos_ent_created_at ON imp_pagos (enterprise_id, created_at);
CREATE INDEX idx_imp_pagos_ent_fecha ON imp_pagos (enterprise_id, fecha);
CREATE INDEX idx_imp_pagos_proveedor ON imp_pagos (proveedor_id);
CREATE INDEX idx_imp_pagos_banco ON imp_pagos (banco_id);
CREATE INDEX idx_imp_pagos_enterprise ON imp_pagos (enterprise_id);
CREATE INDEX idx_imp_pagos_asiento ON imp_pagos (asiento_id);
CREATE INDEX idx_imp_pagos_user ON imp_pagos (user_id);
CREATE INDEX idx_imp_pagos_orden_compra ON imp_pagos (orden_compra_id);

-- Tabla: imp_vessel_tracking
CREATE INDEX idx_imp_vessel_tracking_ent_user ON imp_vessel_tracking (enterprise_id, user_id);
CREATE INDEX idx_imp_vessel_tracking_ent_orden_compra ON imp_vessel_tracking (enterprise_id, orden_compra_id);
CREATE INDEX idx_imp_vessel_tracking_ent_created_at ON imp_vessel_tracking (enterprise_id, created_at);
CREATE INDEX idx_imp_vessel_tracking_user ON imp_vessel_tracking (user_id);

-- Tabla: legacy_libros
CREATE INDEX idx_legacy_libros_ent_user ON legacy_libros (enterprise_id, user_id);
CREATE INDEX idx_legacy_libros_ent_created_at ON legacy_libros (enterprise_id, created_at);
CREATE INDEX idx_legacy_libros_user ON legacy_libros (user_id);

-- Tabla: libros_detalles
CREATE INDEX idx_libros_detalles_ent_libro ON libros_detalles (enterprise_id, libro_id);
CREATE INDEX idx_libros_detalles_ent_user ON libros_detalles (enterprise_id, user_id);
CREATE INDEX idx_libros_detalles_ent_created_at ON libros_detalles (enterprise_id, created_at);
CREATE INDEX idx_libros_detalles_user ON libros_detalles (user_id);

-- Tabla: log_erp_terceros_cm05
CREATE INDEX idx_log_erp_terceros_cm05_tercero ON log_erp_terceros_cm05 (tercero_id);
CREATE INDEX idx_log_erp_terceros_cm05_user ON log_erp_terceros_cm05 (user_id);

-- Tabla: movimientos_pendientes
CREATE INDEX idx_movimientos_pendientes_ent_motivo ON movimientos_pendientes (enterprise_id, motivo_id);
CREATE INDEX idx_movimientos_pendientes_ent_libro ON movimientos_pendientes (enterprise_id, libro_id);
CREATE INDEX idx_movimientos_pendientes_ent_user ON movimientos_pendientes (enterprise_id, user_id);
CREATE INDEX idx_movimientos_pendientes_ent_estado ON movimientos_pendientes (enterprise_id, estado);
CREATE INDEX idx_movimientos_pendientes_ent_created_at ON movimientos_pendientes (enterprise_id, created_at);
CREATE INDEX idx_movimientos_pendientes_user ON movimientos_pendientes (user_id);

-- Tabla: movimientos_stock
CREATE INDEX idx_movimientos_stock_ent_documento ON movimientos_stock (enterprise_id, documento_id);
CREATE INDEX idx_movimientos_stock_ent_usuario ON movimientos_stock (enterprise_id, usuario_id);
CREATE INDEX idx_movimientos_stock_ent_articulo ON movimientos_stock (enterprise_id, articulo_id);
CREATE INDEX idx_movimientos_stock_ent_created_at ON movimientos_stock (enterprise_id, created_at);
CREATE INDEX idx_movimientos_stock_documento ON movimientos_stock (documento_id);
CREATE INDEX idx_movimientos_stock_usuario ON movimientos_stock (usuario_id);

-- Tabla: prd_proyectos_desarrollo
CREATE INDEX idx_prd_proyectos_desarrollo_ent_articulo_objetivo ON prd_proyectos_desarrollo (enterprise_id, articulo_objetivo_id);
CREATE INDEX idx_prd_proyectos_desarrollo_ent_user ON prd_proyectos_desarrollo (enterprise_id, user_id);
CREATE INDEX idx_prd_proyectos_desarrollo_ent_estado ON prd_proyectos_desarrollo (enterprise_id, estado);
CREATE INDEX idx_prd_proyectos_desarrollo_ent_created_at ON prd_proyectos_desarrollo (enterprise_id, created_at);
CREATE INDEX idx_prd_proyectos_desarrollo_articulo_objetivo ON prd_proyectos_desarrollo (articulo_objetivo_id);
CREATE INDEX idx_prd_proyectos_desarrollo_user ON prd_proyectos_desarrollo (user_id);

-- Tabla: prestamos
CREATE INDEX idx_prestamos_ent_usuario ON prestamos (enterprise_id, usuario_id);
CREATE INDEX idx_prestamos_ent_estado ON prestamos (enterprise_id, estado);
CREATE INDEX idx_prestamos_ent_created_at ON prestamos (enterprise_id, created_at);

-- Tabla: proveedores
CREATE INDEX idx_proveedores_ent_user ON proveedores (enterprise_id, user_id);
CREATE INDEX idx_proveedores_ent_activo ON proveedores (enterprise_id, activo);
CREATE INDEX idx_proveedores_ent_created_at ON proveedores (enterprise_id, created_at);
CREATE INDEX idx_proveedores_user ON proveedores (user_id);
CREATE INDEX idx_proveedores_codigo ON proveedores (enterprise_id, codigo);
CREATE INDEX idx_proveedores_email ON proveedores (enterprise_id, email);
CREATE INDEX idx_proveedores_cuit ON proveedores (enterprise_id, cuit);

-- Tabla: service_efficiency
CREATE INDEX idx_service_efficiency_user ON service_efficiency (user_id);

-- Tabla: stk_archivos_digitales
CREATE INDEX idx_stk_archivos_digitales_ent_user ON stk_archivos_digitales (enterprise_id, user_id);
CREATE INDEX idx_stk_archivos_digitales_ent_articulo ON stk_archivos_digitales (enterprise_id, articulo_id);
CREATE INDEX idx_stk_archivos_digitales_ent_created_at ON stk_archivos_digitales (enterprise_id, created_at);
CREATE INDEX idx_stk_archivos_digitales_enterprise ON stk_archivos_digitales (enterprise_id);
CREATE INDEX idx_stk_archivos_digitales_user ON stk_archivos_digitales (user_id);

-- Tabla: stk_articulos
CREATE INDEX idx_stk_articulos_ent_categoria ON stk_articulos (enterprise_id, categoria_id);
CREATE INDEX idx_stk_articulos_ent_tipo_articulo ON stk_articulos (enterprise_id, tipo_articulo_id);
CREATE INDEX idx_stk_articulos_ent_user ON stk_articulos (enterprise_id, user_id);
CREATE INDEX idx_stk_articulos_ent_activo ON stk_articulos (enterprise_id, activo);
CREATE INDEX idx_stk_articulos_ent_created_at ON stk_articulos (enterprise_id, created_at);
CREATE INDEX idx_stk_articulos_categoria ON stk_articulos (categoria_id);
CREATE INDEX idx_stk_articulos_tipo_articulo ON stk_articulos (tipo_articulo_id);
CREATE INDEX idx_stk_articulos_user ON stk_articulos (user_id);
CREATE INDEX idx_stk_articulos_codigo ON stk_articulos (enterprise_id, codigo);

-- Tabla: stk_articulos_codigos
CREATE INDEX idx_stk_articulos_codigos_ent_articulo ON stk_articulos_codigos (enterprise_id, articulo_id);
CREATE INDEX idx_stk_articulos_codigos_ent_created_at ON stk_articulos_codigos (enterprise_id, created_at);

-- Tabla: stk_articulos_precios
CREATE INDEX idx_stk_articulos_precios_ent_lista_precio ON stk_articulos_precios (enterprise_id, lista_precio_id);
CREATE INDEX idx_stk_articulos_precios_ent_regla ON stk_articulos_precios (enterprise_id, regla_id);
CREATE INDEX idx_stk_articulos_precios_ent_user ON stk_articulos_precios (enterprise_id, user_id);
CREATE INDEX idx_stk_articulos_precios_ent_articulo ON stk_articulos_precios (enterprise_id, articulo_id);
CREATE INDEX idx_stk_articulos_precios_ent_created_at ON stk_articulos_precios (enterprise_id, created_at);
CREATE INDEX idx_stk_articulos_precios_enterprise ON stk_articulos_precios (enterprise_id);
CREATE INDEX idx_stk_articulos_precios_user ON stk_articulos_precios (user_id);

-- Tabla: stk_balanzas_config
CREATE INDEX idx_stk_balanzas_config_ent_activo ON stk_balanzas_config (enterprise_id, activo);
CREATE INDEX idx_stk_balanzas_config_ent_created_at ON stk_balanzas_config (enterprise_id, created_at);
CREATE INDEX idx_stk_balanzas_config_numero_serie ON stk_balanzas_config (enterprise_id, numero_serie);

-- Tabla: stk_barcode_rules
CREATE INDEX idx_stk_barcode_rules_ent_activo ON stk_barcode_rules (enterprise_id, activo);
CREATE INDEX idx_stk_barcode_rules_ent_created_at ON stk_barcode_rules (enterprise_id, created_at);

-- Tabla: stk_depositos
CREATE INDEX idx_stk_depositos_ent_tercero ON stk_depositos (enterprise_id, tercero_id);
CREATE INDEX idx_stk_depositos_ent_user ON stk_depositos (enterprise_id, user_id);
CREATE INDEX idx_stk_depositos_ent_activo ON stk_depositos (enterprise_id, activo);
CREATE INDEX idx_stk_depositos_ent_created_at ON stk_depositos (enterprise_id, created_at);
CREATE INDEX idx_stk_depositos_tercero ON stk_depositos (tercero_id);
CREATE INDEX idx_stk_depositos_enterprise ON stk_depositos (enterprise_id);
CREATE INDEX idx_stk_depositos_user ON stk_depositos (user_id);
CREATE INDEX idx_stk_depositos_numero ON stk_depositos (enterprise_id, numero);

-- Tabla: stk_detalles_recepcion
CREATE INDEX idx_stk_detalles_recepcion_ent_recepcion ON stk_detalles_recepcion (enterprise_id, recepcion_id);
CREATE INDEX idx_stk_detalles_recepcion_ent_detalle_orden ON stk_detalles_recepcion (enterprise_id, detalle_orden_id);
CREATE INDEX idx_stk_detalles_recepcion_ent_articulo ON stk_detalles_recepcion (enterprise_id, articulo_id);
CREATE INDEX idx_stk_detalles_recepcion_enterprise ON stk_detalles_recepcion (enterprise_id);

-- Tabla: stk_devoluciones_solicitudes
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_condicion_devolucion ON stk_devoluciones_solicitudes (enterprise_id, condicion_devolucion_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_tercero ON stk_devoluciones_solicitudes (enterprise_id, tercero_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_deposito_destino ON stk_devoluciones_solicitudes (enterprise_id, deposito_destino_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_comprobante_origen ON stk_devoluciones_solicitudes (enterprise_id, comprobante_origen_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_logistica ON stk_devoluciones_solicitudes (enterprise_id, logistica_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_user ON stk_devoluciones_solicitudes (enterprise_id, user_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_estado ON stk_devoluciones_solicitudes (enterprise_id, estado);
CREATE INDEX idx_stk_devoluciones_solicitudes_ent_created_at ON stk_devoluciones_solicitudes (enterprise_id, created_at);
CREATE INDEX idx_stk_devoluciones_solicitudes_condicion_devolucion ON stk_devoluciones_solicitudes (condicion_devolucion_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_tercero ON stk_devoluciones_solicitudes (tercero_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_deposito_destino ON stk_devoluciones_solicitudes (deposito_destino_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_comprobante_origen ON stk_devoluciones_solicitudes (comprobante_origen_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_logistica ON stk_devoluciones_solicitudes (logistica_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_enterprise ON stk_devoluciones_solicitudes (enterprise_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_user ON stk_devoluciones_solicitudes (user_id);

-- Tabla: stk_devoluciones_solicitudes_det
CREATE INDEX idx_stk_devoluciones_solicitudes_det_user ON stk_devoluciones_solicitudes_det (user_id);
CREATE INDEX idx_stk_devoluciones_solicitudes_det_articulo ON stk_devoluciones_solicitudes_det (articulo_id);

-- Tabla: stk_existencias
CREATE INDEX idx_stk_existencias_ent_user ON stk_existencias (enterprise_id, user_id);
CREATE INDEX idx_stk_existencias_ent_created_at ON stk_existencias (enterprise_id, created_at);
CREATE INDEX idx_stk_existencias_user ON stk_existencias (user_id);

-- Tabla: stk_impresoras_config
CREATE INDEX idx_stk_impresoras_config_ent_activo ON stk_impresoras_config (enterprise_id, activo);
CREATE INDEX idx_stk_impresoras_config_ent_created_at ON stk_impresoras_config (enterprise_id, created_at);
CREATE INDEX idx_stk_impresoras_config_enterprise ON stk_impresoras_config (enterprise_id);

-- Tabla: stk_inventarios
CREATE INDEX idx_stk_inventarios_ent_responsable ON stk_inventarios (enterprise_id, responsable_id);
CREATE INDEX idx_stk_inventarios_ent_user ON stk_inventarios (enterprise_id, user_id);
CREATE INDEX idx_stk_inventarios_ent_deposito ON stk_inventarios (enterprise_id, deposito_id);
CREATE INDEX idx_stk_inventarios_ent_estado ON stk_inventarios (enterprise_id, estado);
CREATE INDEX idx_stk_inventarios_ent_created_at ON stk_inventarios (enterprise_id, created_at);
CREATE INDEX idx_stk_inventarios_enterprise ON stk_inventarios (enterprise_id);
CREATE INDEX idx_stk_inventarios_responsable ON stk_inventarios (responsable_id);
CREATE INDEX idx_stk_inventarios_user ON stk_inventarios (user_id);

-- Tabla: stk_items_inventario
CREATE INDEX idx_stk_items_inventario_ent_inventario ON stk_items_inventario (enterprise_id, inventario_id);
CREATE INDEX idx_stk_items_inventario_ent_user ON stk_items_inventario (enterprise_id, user_id);
CREATE INDEX idx_stk_items_inventario_ent_articulo ON stk_items_inventario (enterprise_id, articulo_id);
CREATE INDEX idx_stk_items_inventario_ent_created_at ON stk_items_inventario (enterprise_id, created_at);
CREATE INDEX idx_stk_items_inventario_enterprise ON stk_items_inventario (enterprise_id);
CREATE INDEX idx_stk_items_inventario_user ON stk_items_inventario (user_id);

-- Tabla: stk_items_transferencia
CREATE INDEX idx_stk_items_transferencia_ent_transferencia ON stk_items_transferencia (enterprise_id, transferencia_id);
CREATE INDEX idx_stk_items_transferencia_ent_user ON stk_items_transferencia (enterprise_id, user_id);
CREATE INDEX idx_stk_items_transferencia_ent_articulo ON stk_items_transferencia (enterprise_id, articulo_id);
CREATE INDEX idx_stk_items_transferencia_ent_created_at ON stk_items_transferencia (enterprise_id, created_at);
CREATE INDEX idx_stk_items_transferencia_enterprise ON stk_items_transferencia (enterprise_id);
CREATE INDEX idx_stk_items_transferencia_user ON stk_items_transferencia (user_id);

-- Tabla: stk_liquidaciones_consignacion
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_tercero ON stk_liquidaciones_consignacion (enterprise_id, tercero_id);
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_comprobante ON stk_liquidaciones_consignacion (enterprise_id, comprobante_id);
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_user ON stk_liquidaciones_consignacion (enterprise_id, user_id);
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_deposito ON stk_liquidaciones_consignacion (enterprise_id, deposito_id);
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_estado ON stk_liquidaciones_consignacion (enterprise_id, estado);
CREATE INDEX idx_stk_liquidaciones_consignacion_ent_created_at ON stk_liquidaciones_consignacion (enterprise_id, created_at);
CREATE INDEX idx_stk_liquidaciones_consignacion_tercero ON stk_liquidaciones_consignacion (tercero_id);
CREATE INDEX idx_stk_liquidaciones_consignacion_comprobante ON stk_liquidaciones_consignacion (comprobante_id);
CREATE INDEX idx_stk_liquidaciones_consignacion_user ON stk_liquidaciones_consignacion (user_id);
CREATE INDEX idx_stk_liquidaciones_consignacion_deposito ON stk_liquidaciones_consignacion (deposito_id);

-- Tabla: stk_liquidaciones_consignacion_det
CREATE INDEX idx_stk_liquidaciones_consignacion_det_articulo ON stk_liquidaciones_consignacion_det (articulo_id);

-- Tabla: stk_listas_precios
CREATE INDEX idx_stk_listas_precios_ent_activo ON stk_listas_precios (enterprise_id, activo);
CREATE INDEX idx_stk_listas_precios_ent_created_at ON stk_listas_precios (enterprise_id, created_at);
CREATE INDEX idx_stk_listas_precios_enterprise ON stk_listas_precios (enterprise_id);

-- Tabla: stk_logisticas
CREATE INDEX idx_stk_logisticas_ent_user ON stk_logisticas (enterprise_id, user_id);
CREATE INDEX idx_stk_logisticas_ent_activo ON stk_logisticas (enterprise_id, activo);
CREATE INDEX idx_stk_logisticas_ent_created_at ON stk_logisticas (enterprise_id, created_at);
CREATE INDEX idx_stk_logisticas_enterprise ON stk_logisticas (enterprise_id);
CREATE INDEX idx_stk_logisticas_user ON stk_logisticas (user_id);
CREATE INDEX idx_stk_logisticas_email ON stk_logisticas (enterprise_id, email);
CREATE INDEX idx_stk_logisticas_cuit ON stk_logisticas (enterprise_id, cuit);
CREATE INDEX idx_stk_logisticas_numero ON stk_logisticas (enterprise_id, numero);

-- Tabla: stk_motivos
CREATE INDEX idx_stk_motivos_ent_user ON stk_motivos (enterprise_id, user_id);
CREATE INDEX idx_stk_motivos_ent_created_at ON stk_motivos (enterprise_id, created_at);
CREATE INDEX idx_stk_motivos_enterprise ON stk_motivos (enterprise_id);
CREATE INDEX idx_stk_motivos_user ON stk_motivos (user_id);

-- Tabla: stk_movimientos
CREATE INDEX idx_stk_movimientos_ent_tercero ON stk_movimientos (enterprise_id, tercero_id);
CREATE INDEX idx_stk_movimientos_ent_deposito_origen ON stk_movimientos (enterprise_id, deposito_origen_id);
CREATE INDEX idx_stk_movimientos_ent_motivo ON stk_movimientos (enterprise_id, motivo_id);
CREATE INDEX idx_stk_movimientos_ent_comprobante ON stk_movimientos (enterprise_id, comprobante_id);
CREATE INDEX idx_stk_movimientos_ent_deposito_destino ON stk_movimientos (enterprise_id, deposito_destino_id);
CREATE INDEX idx_stk_movimientos_ent_user ON stk_movimientos (enterprise_id, user_id);
CREATE INDEX idx_stk_movimientos_ent_estado ON stk_movimientos (enterprise_id, estado);
CREATE INDEX idx_stk_movimientos_ent_fecha ON stk_movimientos (enterprise_id, fecha);
CREATE INDEX idx_stk_movimientos_ent_created_at ON stk_movimientos (enterprise_id, created_at);
CREATE INDEX idx_stk_movimientos_comprobante ON stk_movimientos (comprobante_id);
CREATE INDEX idx_stk_movimientos_enterprise ON stk_movimientos (enterprise_id);
CREATE INDEX idx_stk_movimientos_user ON stk_movimientos (user_id);

-- Tabla: stk_movimientos_detalle
CREATE INDEX idx_stk_movimientos_detalle_ent_movimiento ON stk_movimientos_detalle (enterprise_id, movimiento_id);
CREATE INDEX idx_stk_movimientos_detalle_ent_user ON stk_movimientos_detalle (enterprise_id, user_id);
CREATE INDEX idx_stk_movimientos_detalle_ent_articulo ON stk_movimientos_detalle (enterprise_id, articulo_id);
CREATE INDEX idx_stk_movimientos_detalle_ent_created_at ON stk_movimientos_detalle (enterprise_id, created_at);
CREATE INDEX idx_stk_movimientos_detalle_enterprise ON stk_movimientos_detalle (enterprise_id);
CREATE INDEX idx_stk_movimientos_detalle_user ON stk_movimientos_detalle (user_id);
CREATE INDEX idx_stk_movimientos_detalle_articulo ON stk_movimientos_detalle (articulo_id);

-- Tabla: stk_numeros_serie
CREATE INDEX idx_stk_numeros_serie_ent_tercero ON stk_numeros_serie (enterprise_id, tercero_id);
CREATE INDEX idx_stk_numeros_serie_ent_ubicacion ON stk_numeros_serie (enterprise_id, ubicacion_id);
CREATE INDEX idx_stk_numeros_serie_ent_estado ON stk_numeros_serie (enterprise_id, estado);
CREATE INDEX idx_stk_numeros_serie_ent_created_at ON stk_numeros_serie (enterprise_id, created_at);
CREATE INDEX idx_stk_numeros_serie_tercero ON stk_numeros_serie (tercero_id);
CREATE INDEX idx_stk_numeros_serie_ubicacion ON stk_numeros_serie (ubicacion_id);
CREATE INDEX idx_stk_numeros_serie_numero_serie ON stk_numeros_serie (enterprise_id, numero_serie);

-- Tabla: stk_pricing_formulas
CREATE INDEX idx_stk_pricing_formulas_ent_created_at ON stk_pricing_formulas (enterprise_id, created_at);
CREATE INDEX idx_stk_pricing_formulas_enterprise ON stk_pricing_formulas (enterprise_id);

-- Tabla: stk_pricing_propuestas
CREATE INDEX idx_stk_pricing_propuestas_ent_metodo_costeo ON stk_pricing_propuestas (enterprise_id, metodo_costeo_id);
CREATE INDEX idx_stk_pricing_propuestas_ent_articulo ON stk_pricing_propuestas (enterprise_id, articulo_id);
CREATE INDEX idx_stk_pricing_propuestas_ent_lista ON stk_pricing_propuestas (enterprise_id, lista_id);
CREATE INDEX idx_stk_pricing_propuestas_ent_documento_origen ON stk_pricing_propuestas (enterprise_id, documento_origen_id);
CREATE INDEX idx_stk_pricing_propuestas_ent_estado ON stk_pricing_propuestas (enterprise_id, estado);
CREATE INDEX idx_stk_pricing_propuestas_metodo_costeo ON stk_pricing_propuestas (metodo_costeo_id);
CREATE INDEX idx_stk_pricing_propuestas_enterprise ON stk_pricing_propuestas (enterprise_id);
CREATE INDEX idx_stk_pricing_propuestas_documento_origen ON stk_pricing_propuestas (documento_origen_id);

-- Tabla: stk_pricing_reglas
CREATE INDEX idx_stk_pricing_reglas_ent_lista_precio ON stk_pricing_reglas (enterprise_id, lista_precio_id);
CREATE INDEX idx_stk_pricing_reglas_ent_metodo_costo ON stk_pricing_reglas (enterprise_id, metodo_costo_id);
CREATE INDEX idx_stk_pricing_reglas_ent_formula ON stk_pricing_reglas (enterprise_id, formula_id);
CREATE INDEX idx_stk_pricing_reglas_ent_activo ON stk_pricing_reglas (enterprise_id, activo);
CREATE INDEX idx_stk_pricing_reglas_enterprise ON stk_pricing_reglas (enterprise_id);

-- Tabla: stk_recepciones
CREATE INDEX idx_stk_recepciones_ent_orden_compra ON stk_recepciones (enterprise_id, orden_compra_id);
CREATE INDEX idx_stk_recepciones_ent_estado ON stk_recepciones (enterprise_id, estado);
CREATE INDEX idx_stk_recepciones_ent_created_at ON stk_recepciones (enterprise_id, created_at);
CREATE INDEX idx_stk_recepciones_enterprise ON stk_recepciones (enterprise_id);

-- Tabla: stk_series_counter
CREATE INDEX idx_stk_series_counter_ent_created_at ON stk_series_counter (enterprise_id, created_at);

-- Tabla: stk_series_trazabilidad
CREATE INDEX idx_stk_series_trazabilidad_ent_tercero ON stk_series_trazabilidad (enterprise_id, tercero_id);
CREATE INDEX idx_stk_series_trazabilidad_ent_serie ON stk_series_trazabilidad (enterprise_id, serie_id);
CREATE INDEX idx_stk_series_trazabilidad_ent_comprobante ON stk_series_trazabilidad (enterprise_id, comprobante_id);
CREATE INDEX idx_stk_series_trazabilidad_ent_user ON stk_series_trazabilidad (enterprise_id, user_id);
CREATE INDEX idx_stk_series_trazabilidad_ent_deposito ON stk_series_trazabilidad (enterprise_id, deposito_id);
CREATE INDEX idx_stk_series_trazabilidad_ent_fecha ON stk_series_trazabilidad (enterprise_id, fecha);
CREATE INDEX idx_stk_series_trazabilidad_tercero ON stk_series_trazabilidad (tercero_id);
CREATE INDEX idx_stk_series_trazabilidad_comprobante ON stk_series_trazabilidad (comprobante_id);
CREATE INDEX idx_stk_series_trazabilidad_enterprise ON stk_series_trazabilidad (enterprise_id);
CREATE INDEX idx_stk_series_trazabilidad_user ON stk_series_trazabilidad (user_id);
CREATE INDEX idx_stk_series_trazabilidad_deposito ON stk_series_trazabilidad (deposito_id);

-- Tabla: stk_servicios_config
CREATE INDEX idx_stk_servicios_config_ent_user ON stk_servicios_config (enterprise_id, user_id);
CREATE INDEX idx_stk_servicios_config_ent_articulo ON stk_servicios_config (enterprise_id, articulo_id);
CREATE INDEX idx_stk_servicios_config_ent_created_at ON stk_servicios_config (enterprise_id, created_at);
CREATE INDEX idx_stk_servicios_config_user ON stk_servicios_config (user_id);

-- Tabla: stk_tipos_articulo
CREATE INDEX idx_stk_tipos_articulo_ent_user ON stk_tipos_articulo (enterprise_id, user_id);
CREATE INDEX idx_stk_tipos_articulo_ent_activo ON stk_tipos_articulo (enterprise_id, activo);
CREATE INDEX idx_stk_tipos_articulo_ent_created_at ON stk_tipos_articulo (enterprise_id, created_at);
CREATE INDEX idx_stk_tipos_articulo_enterprise ON stk_tipos_articulo (enterprise_id);
CREATE INDEX idx_stk_tipos_articulo_user ON stk_tipos_articulo (user_id);

-- Tabla: stk_tipos_articulo_servicios
CREATE INDEX idx_stk_tipos_articulo_servicios_ent_servicio ON stk_tipos_articulo_servicios (enterprise_id, servicio_id);
CREATE INDEX idx_stk_tipos_articulo_servicios_ent_user ON stk_tipos_articulo_servicios (enterprise_id, user_id);
CREATE INDEX idx_stk_tipos_articulo_servicios_ent_created_at ON stk_tipos_articulo_servicios (enterprise_id, created_at);
CREATE INDEX idx_stk_tipos_articulo_servicios_user ON stk_tipos_articulo_servicios (user_id);

-- Tabla: stk_tipos_articulos
CREATE INDEX idx_stk_tipos_articulos_ent_user ON stk_tipos_articulos (enterprise_id, user_id);
CREATE INDEX idx_stk_tipos_articulos_ent_activo ON stk_tipos_articulos (enterprise_id, activo);
CREATE INDEX idx_stk_tipos_articulos_ent_created_at ON stk_tipos_articulos (enterprise_id, created_at);
CREATE INDEX idx_stk_tipos_articulos_user ON stk_tipos_articulos (user_id);

-- Tabla: stk_transferencias
CREATE INDEX idx_stk_transferencias_ent_origen ON stk_transferencias (enterprise_id, origen_id);
CREATE INDEX idx_stk_transferencias_ent_logistica ON stk_transferencias (enterprise_id, logistica_id);
CREATE INDEX idx_stk_transferencias_ent_usuario ON stk_transferencias (enterprise_id, usuario_id);
CREATE INDEX idx_stk_transferencias_ent_destino ON stk_transferencias (enterprise_id, destino_id);
CREATE INDEX idx_stk_transferencias_ent_estado ON stk_transferencias (enterprise_id, estado);
CREATE INDEX idx_stk_transferencias_ent_fecha ON stk_transferencias (enterprise_id, fecha);
CREATE INDEX idx_stk_transferencias_ent_created_at ON stk_transferencias (enterprise_id, created_at);
CREATE INDEX idx_stk_transferencias_enterprise ON stk_transferencias (enterprise_id);
CREATE INDEX idx_stk_transferencias_logistica ON stk_transferencias (logistica_id);
CREATE INDEX idx_stk_transferencias_usuario ON stk_transferencias (usuario_id);

-- Tabla: stock_ajustes
CREATE INDEX idx_stock_ajustes_ent_motivo ON stock_ajustes (enterprise_id, motivo_id);
CREATE INDEX idx_stock_ajustes_ent_libro ON stock_ajustes (enterprise_id, libro_id);
CREATE INDEX idx_stock_ajustes_ent_user ON stock_ajustes (enterprise_id, user_id);
CREATE INDEX idx_stock_ajustes_ent_fecha ON stock_ajustes (enterprise_id, fecha);
CREATE INDEX idx_stock_ajustes_ent_created_at ON stock_ajustes (enterprise_id, created_at);
CREATE INDEX idx_stock_ajustes_user ON stock_ajustes (user_id);

-- Tabla: stock_motivos
CREATE INDEX idx_stock_motivos_ent_user ON stock_motivos (enterprise_id, user_id);
CREATE INDEX idx_stock_motivos_ent_created_at ON stock_motivos (enterprise_id, created_at);
CREATE INDEX idx_stock_motivos_user ON stock_motivos (user_id);

-- Tabla: sys_active_tasks
CREATE INDEX idx_sys_active_tasks_ent_parent ON sys_active_tasks (enterprise_id, parent_id);
CREATE INDEX idx_sys_active_tasks_ent_task ON sys_active_tasks (enterprise_id, task_id);
CREATE INDEX idx_sys_active_tasks_ent_thread ON sys_active_tasks (enterprise_id, thread_id);
CREATE INDEX idx_sys_active_tasks_ent_user ON sys_active_tasks (enterprise_id, user_id);
CREATE INDEX idx_sys_active_tasks_ent_status ON sys_active_tasks (enterprise_id, status);
CREATE INDEX idx_sys_active_tasks_ent_created_at ON sys_active_tasks (enterprise_id, created_at);
CREATE INDEX idx_sys_active_tasks_enterprise ON sys_active_tasks (enterprise_id);
CREATE INDEX idx_sys_active_tasks_parent ON sys_active_tasks (parent_id);
CREATE INDEX idx_sys_active_tasks_thread ON sys_active_tasks (thread_id);
CREATE INDEX idx_sys_active_tasks_user ON sys_active_tasks (user_id);

-- Tabla: sys_ai_feedback
CREATE INDEX idx_sys_ai_feedback_ent_user ON sys_ai_feedback (enterprise_id, user_id);
CREATE INDEX idx_sys_ai_feedback_ent_created_at ON sys_ai_feedback (enterprise_id, created_at);

-- Tabla: sys_approval_signatures
CREATE INDEX idx_sys_approval_signatures_ent_approval ON sys_approval_signatures (enterprise_id, approval_id);
CREATE INDEX idx_sys_approval_signatures_ent_user ON sys_approval_signatures (enterprise_id, user_id);
CREATE INDEX idx_sys_approval_signatures_enterprise ON sys_approval_signatures (enterprise_id);

-- Tabla: sys_budget_execution
CREATE INDEX idx_sys_budget_execution_ent_budget ON sys_budget_execution (enterprise_id, budget_id);
CREATE INDEX idx_sys_budget_execution_ent_transaction ON sys_budget_execution (enterprise_id, transaction_id);
CREATE INDEX idx_sys_budget_execution_ent_created_at ON sys_budget_execution (enterprise_id, created_at);
CREATE INDEX idx_sys_budget_execution_enterprise ON sys_budget_execution (enterprise_id);
CREATE INDEX idx_sys_budget_execution_transaction ON sys_budget_execution (transaction_id);

-- Tabla: sys_budgets
CREATE INDEX idx_sys_budgets_ent_status ON sys_budgets (enterprise_id, status);

-- Tabla: sys_config_fiscal
CREATE INDEX idx_sys_config_fiscal_ent_firma_digital ON sys_config_fiscal (enterprise_id, firma_digital_id);
CREATE INDEX idx_sys_config_fiscal_ent_user ON sys_config_fiscal (enterprise_id, user_id);
CREATE INDEX idx_sys_config_fiscal_ent_created_at ON sys_config_fiscal (enterprise_id, created_at);
CREATE INDEX idx_sys_config_fiscal_firma_digital ON sys_config_fiscal (firma_digital_id);
CREATE INDEX idx_sys_config_fiscal_user ON sys_config_fiscal (user_id);

-- Tabla: sys_cost_centers
CREATE INDEX idx_sys_cost_centers_ent_parent ON sys_cost_centers (enterprise_id, parent_id);
CREATE INDEX idx_sys_cost_centers_ent_created_at ON sys_cost_centers (enterprise_id, created_at);

-- Tabla: sys_crons
CREATE INDEX idx_sys_crons_ent_user ON sys_crons (enterprise_id, user_id);
CREATE INDEX idx_sys_crons_ent_estado ON sys_crons (enterprise_id, estado);
CREATE INDEX idx_sys_crons_ent_created_at ON sys_crons (enterprise_id, created_at);
CREATE INDEX idx_sys_crons_enterprise ON sys_crons (enterprise_id);
CREATE INDEX idx_sys_crons_user ON sys_crons (user_id);

-- Tabla: sys_crons_logs
CREATE INDEX idx_sys_crons_logs_user ON sys_crons_logs (user_id);

-- Tabla: sys_departamentos
CREATE INDEX idx_sys_departamentos_user ON sys_departamentos (user_id);

-- Tabla: sys_documentos_adjuntos
CREATE INDEX idx_sys_documentos_adjuntos_ent_user ON sys_documentos_adjuntos (enterprise_id, user_id);
CREATE INDEX idx_sys_documentos_adjuntos_ent_entidad ON sys_documentos_adjuntos (enterprise_id, entidad_id);
CREATE INDEX idx_sys_documentos_adjuntos_ent_estado ON sys_documentos_adjuntos (enterprise_id, estado);
CREATE INDEX idx_sys_documentos_adjuntos_ent_created_at ON sys_documentos_adjuntos (enterprise_id, created_at);
CREATE INDEX idx_sys_documentos_adjuntos_ent_fecha_emision ON sys_documentos_adjuntos (enterprise_id, fecha_emision);
CREATE INDEX idx_sys_documentos_adjuntos_user ON sys_documentos_adjuntos (user_id);

-- Tabla: sys_enrichment_counters
CREATE INDEX idx_sys_enrichment_counters_user ON sys_enrichment_counters (user_id);

-- Tabla: sys_enterprise_logos
CREATE INDEX idx_sys_enterprise_logos_ent_user ON sys_enterprise_logos (enterprise_id, user_id);
CREATE INDEX idx_sys_enterprise_logos_ent_created_at ON sys_enterprise_logos (enterprise_id, created_at);
CREATE INDEX idx_sys_enterprise_logos_user ON sys_enterprise_logos (user_id);

-- Tabla: sys_enterprise_numeracion
CREATE INDEX idx_sys_enterprise_numeracion_ent_user ON sys_enterprise_numeracion (enterprise_id, user_id);
CREATE INDEX idx_sys_enterprise_numeracion_ent_created_at ON sys_enterprise_numeracion (enterprise_id, created_at);
CREATE INDEX idx_sys_enterprise_numeracion_user ON sys_enterprise_numeracion (user_id);

-- Tabla: sys_enterprises
CREATE INDEX idx_sys_enterprises_user ON sys_enterprises (user_id);
CREATE INDEX idx_sys_enterprises_cuit ON sys_enterprises (cuit);

-- Tabla: sys_enterprises_fiscal
CREATE INDEX idx_sys_enterprises_fiscal_ent_user ON sys_enterprises_fiscal (enterprise_id, user_id);
CREATE INDEX idx_sys_enterprises_fiscal_ent_activo ON sys_enterprises_fiscal (enterprise_id, activo);
CREATE INDEX idx_sys_enterprises_fiscal_ent_created_at ON sys_enterprises_fiscal (enterprise_id, created_at);
CREATE INDEX idx_sys_enterprises_fiscal_user ON sys_enterprises_fiscal (user_id);

-- Tabla: sys_enterprises_new
CREATE INDEX idx_sys_enterprises_new_user ON sys_enterprises_new (user_id);

-- Tabla: sys_external_services
CREATE INDEX idx_sys_external_services_ent_user ON sys_external_services (enterprise_id, user_id);
CREATE INDEX idx_sys_external_services_ent_activo ON sys_external_services (enterprise_id, activo);
CREATE INDEX idx_sys_external_services_ent_created_at ON sys_external_services (enterprise_id, created_at);
CREATE INDEX idx_sys_external_services_enterprise ON sys_external_services (enterprise_id);
CREATE INDEX idx_sys_external_services_user ON sys_external_services (user_id);

-- Tabla: sys_fiscal_comprobante_rules
CREATE INDEX idx_sys_fiscal_comprobante_rules_user ON sys_fiscal_comprobante_rules (user_id);

-- Tabla: sys_impuestos
CREATE INDEX idx_sys_impuestos_ent_user ON sys_impuestos (enterprise_id, user_id);
CREATE INDEX idx_sys_impuestos_ent_activo ON sys_impuestos (enterprise_id, activo);
CREATE INDEX idx_sys_impuestos_ent_created_at ON sys_impuestos (enterprise_id, created_at);
CREATE INDEX idx_sys_impuestos_user ON sys_impuestos (user_id);

-- Tabla: sys_invoice_layouts
CREATE INDEX idx_sys_invoice_layouts_ent_user ON sys_invoice_layouts (enterprise_id, user_id);
CREATE INDEX idx_sys_invoice_layouts_ent_created_at ON sys_invoice_layouts (enterprise_id, created_at);
CREATE INDEX idx_sys_invoice_layouts_user ON sys_invoice_layouts (user_id);

-- Tabla: sys_jurisdicciones
CREATE INDEX idx_sys_jurisdicciones_user ON sys_jurisdicciones (user_id);

-- Tabla: sys_jurisdicciones_iibb
CREATE INDEX idx_sys_jurisdicciones_iibb_user ON sys_jurisdicciones_iibb (user_id);

-- Tabla: sys_localidades
CREATE INDEX idx_sys_localidades_municipio ON sys_localidades (municipio_id);
CREATE INDEX idx_sys_localidades_user ON sys_localidades (user_id);

-- Tabla: sys_municipios
CREATE INDEX idx_sys_municipios_user ON sys_municipios (user_id);

-- Tabla: sys_padrones_iibb
CREATE INDEX idx_sys_padrones_iibb_user ON sys_padrones_iibb (user_id);

-- Tabla: sys_padrones_logs
CREATE INDEX idx_sys_padrones_logs_user ON sys_padrones_logs (user_id);

-- Tabla: sys_permissions
CREATE INDEX idx_sys_permissions_ent_user ON sys_permissions (enterprise_id, user_id);
CREATE INDEX idx_sys_permissions_ent_created_at ON sys_permissions (enterprise_id, created_at);
CREATE INDEX idx_sys_permissions_user ON sys_permissions (user_id);

-- Tabla: sys_provincias
CREATE INDEX idx_sys_provincias_iso ON sys_provincias (iso_id);
CREATE INDEX idx_sys_provincias_user ON sys_provincias (user_id);

-- Tabla: sys_risk_active_mitigations
CREATE INDEX idx_sys_risk_active_mitigations_ent_rule ON sys_risk_active_mitigations (enterprise_id, rule_id);
CREATE INDEX idx_sys_risk_active_mitigations_ent_user ON sys_risk_active_mitigations (enterprise_id, user_id);
CREATE INDEX idx_sys_risk_active_mitigations_ent_target_user ON sys_risk_active_mitigations (enterprise_id, target_user_id);
CREATE INDEX idx_sys_risk_active_mitigations_ent_status ON sys_risk_active_mitigations (enterprise_id, status);
CREATE INDEX idx_sys_risk_active_mitigations_ent_created_at ON sys_risk_active_mitigations (enterprise_id, created_at);
CREATE INDEX idx_sys_risk_active_mitigations_rule ON sys_risk_active_mitigations (rule_id);
CREATE INDEX idx_sys_risk_active_mitigations_enterprise ON sys_risk_active_mitigations (enterprise_id);
CREATE INDEX idx_sys_risk_active_mitigations_user ON sys_risk_active_mitigations (user_id);
CREATE INDEX idx_sys_risk_active_mitigations_target_user ON sys_risk_active_mitigations (target_user_id);

-- Tabla: sys_risk_mitigation_rules
CREATE INDEX idx_sys_risk_mitigation_rules_ent_user ON sys_risk_mitigation_rules (enterprise_id, user_id);
CREATE INDEX idx_sys_risk_mitigation_rules_ent_created_at ON sys_risk_mitigation_rules (enterprise_id, created_at);
CREATE INDEX idx_sys_risk_mitigation_rules_enterprise ON sys_risk_mitigation_rules (enterprise_id);
CREATE INDEX idx_sys_risk_mitigation_rules_user ON sys_risk_mitigation_rules (user_id);

-- Tabla: sys_role_permissions
CREATE INDEX idx_sys_role_permissions_ent_permission ON sys_role_permissions (enterprise_id, permission_id);
CREATE INDEX idx_sys_role_permissions_ent_role ON sys_role_permissions (enterprise_id, role_id);
CREATE INDEX idx_sys_role_permissions_ent_user ON sys_role_permissions (enterprise_id, user_id);
CREATE INDEX idx_sys_role_permissions_ent_created_at ON sys_role_permissions (enterprise_id, created_at);
CREATE INDEX idx_sys_role_permissions_user ON sys_role_permissions (user_id);

-- Tabla: sys_roles
CREATE INDEX idx_sys_roles_ent_user ON sys_roles (enterprise_id, user_id);
CREATE INDEX idx_sys_roles_ent_created_at ON sys_roles (enterprise_id, created_at);
CREATE INDEX idx_sys_roles_user ON sys_roles (user_id);

-- Tabla: sys_security_logs
CREATE INDEX idx_sys_security_logs_ent_actor_user ON sys_security_logs (enterprise_id, actor_user_id);
CREATE INDEX idx_sys_security_logs_ent_session ON sys_security_logs (enterprise_id, session_id);
CREATE INDEX idx_sys_security_logs_ent_user ON sys_security_logs (enterprise_id, user_id);
CREATE INDEX idx_sys_security_logs_ent_target_user ON sys_security_logs (enterprise_id, target_user_id);
CREATE INDEX idx_sys_security_logs_ent_status ON sys_security_logs (enterprise_id, status);
CREATE INDEX idx_sys_security_logs_ent_created_at ON sys_security_logs (enterprise_id, created_at);
CREATE INDEX idx_sys_security_logs_session ON sys_security_logs (session_id);
CREATE INDEX idx_sys_security_logs_user ON sys_security_logs (user_id);

-- Tabla: sys_tipos_comprobante
CREATE INDEX idx_sys_tipos_comprobante_ent_user ON sys_tipos_comprobante (enterprise_id, user_id);
CREATE INDEX idx_sys_tipos_comprobante_ent_created_at ON sys_tipos_comprobante (enterprise_id, created_at);
CREATE INDEX idx_sys_tipos_comprobante_user ON sys_tipos_comprobante (user_id);

-- Tabla: sys_transaction_approvals
CREATE INDEX idx_sys_transaction_approvals_ent_rule ON sys_transaction_approvals (enterprise_id, rule_id);
CREATE INDEX idx_sys_transaction_approvals_ent_transaction ON sys_transaction_approvals (enterprise_id, transaction_id);
CREATE INDEX idx_sys_transaction_approvals_ent_status ON sys_transaction_approvals (enterprise_id, status);
CREATE INDEX idx_sys_transaction_approvals_ent_created_at ON sys_transaction_approvals (enterprise_id, created_at);

-- Tabla: sys_transaction_logs
CREATE INDEX idx_sys_transaction_logs_ent_user ON sys_transaction_logs (enterprise_id, user_id);
CREATE INDEX idx_sys_transaction_logs_ent_status ON sys_transaction_logs (enterprise_id, status);
CREATE INDEX idx_sys_transaction_logs_ent_created_at ON sys_transaction_logs (enterprise_id, created_at);
CREATE INDEX idx_sys_transaction_logs_user ON sys_transaction_logs (user_id);

-- Tabla: sys_users
CREATE INDEX idx_sys_users_ent_role ON sys_users (enterprise_id, role_id);
CREATE INDEX idx_sys_users_ent_created_at ON sys_users (enterprise_id, created_at);
CREATE INDEX idx_sys_users_email ON sys_users (enterprise_id, email);

-- Tabla: sys_workflow_rules
CREATE INDEX idx_sys_workflow_rules_ent_created_at ON sys_workflow_rules (enterprise_id, created_at);

-- Tabla: sys_workflow_steps
CREATE INDEX idx_sys_workflow_steps_ent_role ON sys_workflow_steps (enterprise_id, role_id);
CREATE INDEX idx_sys_workflow_steps_ent_rule ON sys_workflow_steps (enterprise_id, rule_id);
CREATE INDEX idx_sys_workflow_steps_ent_user ON sys_workflow_steps (enterprise_id, user_id);
CREATE INDEX idx_sys_workflow_steps_role ON sys_workflow_steps (role_id);
CREATE INDEX idx_sys_workflow_steps_enterprise ON sys_workflow_steps (enterprise_id);
CREATE INDEX idx_sys_workflow_steps_user ON sys_workflow_steps (user_id);

-- Tabla: system_stats
CREATE INDEX idx_system_stats_ent_user ON system_stats (enterprise_id, user_id);
CREATE INDEX idx_system_stats_ent_created_at ON system_stats (enterprise_id, created_at);
CREATE INDEX idx_system_stats_user ON system_stats (user_id);

-- Tabla: tax_alicuotas
CREATE INDEX idx_tax_alicuotas_ent_user ON tax_alicuotas (enterprise_id, user_id);
CREATE INDEX idx_tax_alicuotas_ent_activo ON tax_alicuotas (enterprise_id, activo);
CREATE INDEX idx_tax_alicuotas_ent_created_at ON tax_alicuotas (enterprise_id, created_at);
CREATE INDEX idx_tax_alicuotas_user ON tax_alicuotas (user_id);

-- Tabla: tax_engine_snapshots
CREATE INDEX idx_tax_engine_snapshots_ent_version ON tax_engine_snapshots (enterprise_id, version_id);
CREATE INDEX idx_tax_engine_snapshots_ent_user ON tax_engine_snapshots (enterprise_id, user_id);
CREATE INDEX idx_tax_engine_snapshots_ent_created_at ON tax_engine_snapshots (enterprise_id, created_at);
CREATE INDEX idx_tax_engine_snapshots_user ON tax_engine_snapshots (user_id);

-- Tabla: tax_engine_versions
CREATE INDEX idx_tax_engine_versions_ent_usuario ON tax_engine_versions (enterprise_id, usuario_id);
CREATE INDEX idx_tax_engine_versions_ent_created_at ON tax_engine_versions (enterprise_id, created_at);
CREATE INDEX idx_tax_engine_versions_usuario ON tax_engine_versions (usuario_id);

-- Tabla: tax_impuestos
CREATE INDEX idx_tax_impuestos_ent_user ON tax_impuestos (enterprise_id, user_id);
CREATE INDEX idx_tax_impuestos_ent_activo ON tax_impuestos (enterprise_id, activo);
CREATE INDEX idx_tax_impuestos_ent_created_at ON tax_impuestos (enterprise_id, created_at);
CREATE INDEX idx_tax_impuestos_user ON tax_impuestos (user_id);
CREATE INDEX idx_tax_impuestos_codigo ON tax_impuestos (enterprise_id, codigo);

-- Tabla: tax_reglas
CREATE INDEX idx_tax_reglas_ent_user ON tax_reglas (enterprise_id, user_id);
CREATE INDEX idx_tax_reglas_ent_impuesto ON tax_reglas (enterprise_id, impuesto_id);
CREATE INDEX idx_tax_reglas_ent_activo ON tax_reglas (enterprise_id, activo);
CREATE INDEX idx_tax_reglas_ent_created_at ON tax_reglas (enterprise_id, created_at);
CREATE INDEX idx_tax_reglas_user ON tax_reglas (user_id);

-- Tabla: tax_reglas_iibb
CREATE INDEX idx_tax_reglas_iibb_ent_user ON tax_reglas_iibb (enterprise_id, user_id);
CREATE INDEX idx_tax_reglas_iibb_ent_impuesto ON tax_reglas_iibb (enterprise_id, impuesto_id);
CREATE INDEX idx_tax_reglas_iibb_ent_activo ON tax_reglas_iibb (enterprise_id, activo);
CREATE INDEX idx_tax_reglas_iibb_ent_created_at ON tax_reglas_iibb (enterprise_id, created_at);
CREATE INDEX idx_tax_reglas_iibb_user ON tax_reglas_iibb (user_id);

-- Tabla: usuarios
CREATE INDEX idx_usuarios_ent_user ON usuarios (enterprise_id, user_id);
CREATE INDEX idx_usuarios_ent_created_at ON usuarios (enterprise_id, created_at);
CREATE INDEX idx_usuarios_user ON usuarios (user_id);
CREATE INDEX idx_usuarios_email ON usuarios (enterprise_id, email);
