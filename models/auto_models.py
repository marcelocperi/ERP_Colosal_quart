from sqlalchemy import Column, Integer, String, Boolean, Text, DateTime, ForeignKey, Enum, Numeric, JSON
from multiMCP.database import Base

class Clientes(Base):
    __tablename__ = 'clientes'

    id = Column(INTEGER, primary_key=True)
    codigo = Column(VARCHAR, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    email = Column(VARCHAR, primary_key=False)
    telefono = Column(VARCHAR, primary_key=False)
    direccion = Column(TEXT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    cuit = Column(VARCHAR, primary_key=False)
    localidad = Column(VARCHAR, primary_key=False)
    tipo_responsable = Column(ENUM, primary_key=False)
    activo = Column(TINYINT, primary_key=False)

class CmpCotizaciones(Base):
    __tablename__ = 'cmp_cotizaciones'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    solicitud_origen_id = Column(INTEGER, primary_key=False)
    proveedor_id = Column(INTEGER, primary_key=False)
    fecha_envio = Column(DATETIME, primary_key=False)
    fecha_vencimiento = Column(DATETIME, primary_key=False)
    estado = Column(ENUM, primary_key=False)
    security_hash = Column(VARCHAR, primary_key=False)
    hash_link = Column(VARCHAR, primary_key=False)

class CmpSolicitudesReposicion(Base):
    __tablename__ = 'cmp_solicitudes_reposicion'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    fecha = Column(DATETIME, primary_key=False)
    solicitante_id = Column(INTEGER, primary_key=False)
    aprobador_id = Column(INTEGER, primary_key=False)
    estado = Column(ENUM, primary_key=False)
    prioridad = Column(ENUM, primary_key=False)
    observaciones = Column(TEXT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class Proveedores(Base):
    __tablename__ = 'proveedores'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    codigo = Column(VARCHAR, primary_key=False)
    razon_social = Column(VARCHAR, primary_key=False)
    naturaleza = Column(VARCHAR, primary_key=False)
    cuit = Column(VARCHAR, primary_key=False)
    direccion = Column(VARCHAR, primary_key=False)
    localidad = Column(VARCHAR, primary_key=False)
    condicion_iva = Column(ENUM, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    telefono = Column(VARCHAR, primary_key=False)
    email = Column(VARCHAR, primary_key=False)

class CmpDetallesOrden(Base):
    __tablename__ = 'cmp_detalles_orden'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    orden_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    cantidad_solicitada = Column(INTEGER, primary_key=False)
    cantidad_recibida = Column(INTEGER, primary_key=False)
    precio_unitario = Column(DECIMAL, primary_key=False)
    subtotal = Column(DECIMAL, primary_key=False)

class CmpOrdenesCompra(Base):
    __tablename__ = 'cmp_ordenes_compra'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    proveedor_id = Column(INTEGER, primary_key=False)
    cotizacion_id = Column(INTEGER, primary_key=False)
    fecha = Column(DATETIME, primary_key=False)
    fecha_emision = Column(DATETIME, primary_key=False)
    estado = Column(ENUM, primary_key=False)
    aprobador_compras_id = Column(INTEGER, primary_key=False)
    fecha_aprobacion_compras = Column(DATETIME, primary_key=False)
    aprobador_tesoreria_id = Column(INTEGER, primary_key=False)
    fecha_aprobacion_tesoreria = Column(DATETIME, primary_key=False)
    fecha_pago_estimada = Column(DATE, primary_key=False)
    observaciones_rechazo = Column(TEXT, primary_key=False)
    security_hash = Column(VARCHAR, primary_key=False)
    observaciones = Column(TEXT, primary_key=False)
    total = Column(DECIMAL, primary_key=False)
    total_estimado = Column(DECIMAL, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    updated_at = Column(TIMESTAMP, primary_key=False)

class StkArticulos(Base):
    __tablename__ = 'stk_articulos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    codigo = Column(VARCHAR, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    descripcion = Column(TEXT, primary_key=False)
    marca = Column(VARCHAR, primary_key=False)
    modelo = Column(VARCHAR, primary_key=False)
    categoria_id = Column(INTEGER, primary_key=False)
    tipo_articulo_id = Column(INTEGER, primary_key=False)
    tipo_articulo = Column(ENUM, primary_key=False)
    unidad_medida = Column(VARCHAR, primary_key=False)
    precio_venta = Column(DECIMAL, primary_key=False)
    api_checked = Column(INTEGER, primary_key=False)
    costo_promedio = Column(DECIMAL, primary_key=False)
    stock_minimo = Column(INTEGER, primary_key=False)
    punto_pedido = Column(INTEGER, primary_key=False)
    metadata_json = Column(LONGTEXT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    updated_at = Column(TIMESTAMP, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    es_comprable = Column(TINYINT, primary_key=False)
    es_prestable = Column(TINYINT, primary_key=False)
    es_recurrente = Column(TINYINT, primary_key=False)
    requiere_serie = Column(TINYINT, primary_key=False)
    patron_serie = Column(VARCHAR, primary_key=False)
    validacion_serie_regex = Column(VARCHAR, primary_key=False)
    longitud_serie_min = Column(INTEGER, primary_key=False)
    longitud_serie_max = Column(INTEGER, primary_key=False)
    prefijo_serie = Column(VARCHAR, primary_key=False)
    genera_serie_automatica = Column(TINYINT, primary_key=False)
    ultimo_numero_serie = Column(BIGINT, primary_key=False)
    es_vendible = Column(TINYINT, primary_key=False)
    naturaleza = Column(ENUM, primary_key=False)
    config_servicio_json = Column(LONGTEXT, primary_key=False)
    costo = Column(DECIMAL, primary_key=False)

class CmpDetallesSolicitud(Base):
    __tablename__ = 'cmp_detalles_solicitud'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    solicitud_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    cantidad_sugerida = Column(INTEGER, primary_key=False)
    cantidad_aprobada = Column(INTEGER, primary_key=False)
    motivo_ajuste = Column(VARCHAR, primary_key=False)

class CmpItemsCotizacion(Base):
    __tablename__ = 'cmp_items_cotizacion'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    cotizacion_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    cantidad = Column(INTEGER, primary_key=False)
    cantidad_ofrecida = Column(INTEGER, primary_key=False)
    precio_ofrecido = Column(DECIMAL, primary_key=False)
    precio_cotizado = Column(DECIMAL, primary_key=False)
    fecha_entrega_estimada = Column(DATE, primary_key=False)

class ContAsientos(Base):
    __tablename__ = 'cont_asientos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    fecha = Column(DATE, primary_key=False)
    concepto = Column(VARCHAR, primary_key=False)
    modulo_origen = Column(VARCHAR, primary_key=False)
    comprobante_id = Column(INTEGER, primary_key=False)
    numero_asiento = Column(INTEGER, primary_key=False)
    estado = Column(VARCHAR, primary_key=False)
    user_id = Column(INTEGER, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class ContAsientosDetalle(Base):
    __tablename__ = 'cont_asientos_detalle'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    asiento_id = Column(INTEGER, primary_key=False)
    cuenta_id = Column(INTEGER, primary_key=False)
    debe = Column(DECIMAL, primary_key=False)
    haber = Column(DECIMAL, primary_key=False)
    glosa = Column(VARCHAR, primary_key=False)

class ContPlanCuentas(Base):
    __tablename__ = 'cont_plan_cuentas'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    codigo = Column(VARCHAR, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    tipo = Column(VARCHAR, primary_key=False)
    imputable = Column(TINYINT, primary_key=False)
    padre_id = Column(INTEGER, primary_key=False)
    nivel = Column(INTEGER, primary_key=False)
    es_analitica = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class CotizacionDolar(Base):
    __tablename__ = 'cotizacion_dolar'

    ent_id_new = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=False)
    compra = Column(DECIMAL, primary_key=False)
    venta = Column(DECIMAL, primary_key=False)
    casa = Column(VARCHAR, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    moneda = Column(VARCHAR, primary_key=False)
    fechaActualizacion = Column(VARCHAR, primary_key=False)
    fecha_registro = Column(DATE, primary_key=False)
    origen = Column(VARCHAR, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=False)

class SysEnterprises(Base):
    __tablename__ = 'sys_enterprises'

    id = Column(INTEGER, primary_key=True)
    codigo = Column(VARCHAR, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    estado = Column(VARCHAR, primary_key=False)
    fecha_creacion = Column(DATETIME, primary_key=False)
    logo_path = Column(VARCHAR, primary_key=False)
    cuit = Column(VARCHAR, primary_key=False)
    domicilio = Column(VARCHAR, primary_key=False)
    condicion_iva = Column(VARCHAR, primary_key=False)
    ingresos_brutos = Column(VARCHAR, primary_key=False)
    inicio_actividades = Column(DATE, primary_key=False)
    email = Column(VARCHAR, primary_key=False)
    telefono = Column(VARCHAR, primary_key=False)
    website = Column(VARCHAR, primary_key=False)
    lema = Column(VARCHAR, primary_key=False)
    iibb_condicion = Column(VARCHAR, primary_key=False)
    is_saas_owner = Column(TINYINT, primary_key=False)
    afip_crt = Column(TEXT, primary_key=False)
    afip_key = Column(TEXT, primary_key=False)
    afip_entorno = Column(VARCHAR, primary_key=False)
    afip_puesto = Column(INTEGER, primary_key=False)

class Enterprises(Base):
    __tablename__ = 'enterprises'

    id = Column(INTEGER, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    cuit = Column(VARCHAR, primary_key=False)
    email_sender = Column(VARCHAR, primary_key=False)
    email_app_password = Column(VARCHAR, primary_key=False)
    direccion = Column(TEXT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class ErpComprobantes(Base):
    __tablename__ = 'erp_comprobantes'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    modulo = Column(VARCHAR, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    tipo_comprobante = Column(VARCHAR, primary_key=False)
    punto_venta = Column(INTEGER, primary_key=False)
    numero = Column(INTEGER, primary_key=False)
    fecha_emision = Column(DATE, primary_key=False)
    fecha_vencimiento = Column(DATE, primary_key=False)
    importe_neto = Column(DECIMAL, primary_key=False)
    importe_iva = Column(DECIMAL, primary_key=False)
    importe_total = Column(DECIMAL, primary_key=False)
    importe_exento = Column(DECIMAL, primary_key=False)
    importe_no_gravado = Column(DECIMAL, primary_key=False)
    importe_percepcion_iva = Column(DECIMAL, primary_key=False)
    importe_percepcion_iibb_arba = Column(DECIMAL, primary_key=False)
    importe_percepcion_iibb_agip = Column(DECIMAL, primary_key=False)
    jurisdiccion_id = Column(INTEGER, primary_key=False)
    importe_impuestos_internos = Column(DECIMAL, primary_key=False)
    estado_pago = Column(VARCHAR, primary_key=False)
    cae = Column(VARCHAR, primary_key=False)
    vto_cae = Column(DATE, primary_key=False)
    asiento_id = Column(INTEGER, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    direccion_entrega_id = Column(INTEGER, primary_key=False)
    receptor_contacto_id = Column(INTEGER, primary_key=False)
    referencia_comercial = Column(VARCHAR, primary_key=False)
    condicion_pago_id = Column(INTEGER, primary_key=False)

class ErpTerceros(Base):
    __tablename__ = 'erp_terceros'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    codigo = Column(VARCHAR, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    naturaleza = Column(VARCHAR, primary_key=False)
    cuit = Column(VARCHAR, primary_key=False)
    tipo_responsable = Column(VARCHAR, primary_key=False)
    condicion_iibb = Column(VARCHAR, primary_key=False)
    iibb_condicion = Column(VARCHAR, primary_key=False)
    observaciones = Column(TEXT, primary_key=False)
    telefono = Column(VARCHAR, primary_key=False)
    email = Column(VARCHAR, primary_key=False)
    es_cliente = Column(TINYINT, primary_key=False)
    es_proveedor = Column(TINYINT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    condicion_pago_id = Column(INTEGER, primary_key=False)
    condicion_pago_pendiente_id = Column(INTEGER, primary_key=False)
    estado_aprobacion_pago = Column(ENUM, primary_key=False)
    id_gerente_aprobador = Column(INTEGER, primary_key=False)
    fecha_aprobacion_pago = Column(DATETIME, primary_key=False)
    condicion_mixta_id = Column(INTEGER, primary_key=False)
    afip_last_check = Column(DATETIME, primary_key=False)
    afip_data = Column(LONGTEXT, primary_key=False)

class FinCondicionesPagoMixtas(Base):
    __tablename__ = 'fin_condiciones_pago_mixtas'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    descripcion = Column(TEXT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class SysJurisdicciones(Base):
    __tablename__ = 'sys_jurisdicciones'

    codigo = Column(INTEGER, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    abreviatura = Column(VARCHAR, primary_key=False)

class ErpComprobantesDetalle(Base):
    __tablename__ = 'erp_comprobantes_detalle'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    comprobante_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    descripcion = Column(VARCHAR, primary_key=False)
    cantidad = Column(DECIMAL, primary_key=False)
    precio_unitario = Column(DECIMAL, primary_key=False)
    alicuota_iva = Column(DECIMAL, primary_key=False)
    subtotal_neto = Column(DECIMAL, primary_key=False)
    importe_iva = Column(DECIMAL, primary_key=False)
    subtotal_total = Column(DECIMAL, primary_key=False)

class ErpComprobantesImpuestos(Base):
    __tablename__ = 'erp_comprobantes_impuestos'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    comprobante_id = Column(INTEGER, primary_key=False)
    jurisdiccion = Column(VARCHAR, primary_key=False)
    alicuota = Column(DECIMAL, primary_key=False)
    base_imponible = Column(DECIMAL, primary_key=False)
    importe = Column(DECIMAL, primary_key=False)

class ErpContactos(Base):
    __tablename__ = 'erp_contactos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    puesto_id = Column(INTEGER, primary_key=False)
    puesto = Column(VARCHAR, primary_key=False)
    tipo_contacto = Column(VARCHAR, primary_key=False)
    telefono = Column(VARCHAR, primary_key=False)
    email = Column(VARCHAR, primary_key=False)
    es_receptor = Column(TINYINT, primary_key=False)
    direccion_id = Column(INTEGER, primary_key=False)

class ErpPuestos(Base):
    __tablename__ = 'erp_puestos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    area = Column(VARCHAR, primary_key=False)
    activo = Column(TINYINT, primary_key=False)

class ErpDirecciones(Base):
    __tablename__ = 'erp_direcciones'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    etiqueta = Column(VARCHAR, primary_key=False)
    calle = Column(VARCHAR, primary_key=False)
    numero = Column(VARCHAR, primary_key=False)
    piso = Column(VARCHAR, primary_key=False)
    depto = Column(VARCHAR, primary_key=False)
    localidad = Column(VARCHAR, primary_key=False)
    municipio = Column(VARCHAR, primary_key=False)
    provincia = Column(VARCHAR, primary_key=False)
    pais = Column(VARCHAR, primary_key=False)
    cod_postal = Column(VARCHAR, primary_key=False)
    es_fiscal = Column(TINYINT, primary_key=False)
    es_entrega = Column(TINYINT, primary_key=False)

class ErpCuentasFondos(Base):
    __tablename__ = 'erp_cuentas_fondos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    tipo = Column(VARCHAR, primary_key=False)
    moneda = Column(VARCHAR, primary_key=False)
    cbu_alias = Column(VARCHAR, primary_key=False)
    saldo_actual = Column(DECIMAL, primary_key=False)
    cuenta_contable_id = Column(INTEGER, primary_key=False)
    activo = Column(TINYINT, primary_key=False)

class ErpDatosFiscales(Base):
    __tablename__ = 'erp_datos_fiscales'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    impuesto = Column(VARCHAR, primary_key=False)
    jurisdiccion = Column(VARCHAR, primary_key=False)
    condicion = Column(VARCHAR, primary_key=False)
    numero_inscripcion = Column(VARCHAR, primary_key=False)
    fecha_vencimiento = Column(DATE, primary_key=False)
    alicuota = Column(DECIMAL, primary_key=False)

class ErpMovimientosFondos(Base):
    __tablename__ = 'erp_movimientos_fondos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    fecha = Column(DATE, primary_key=False)
    tipo = Column(VARCHAR, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    cuenta_fondo_id = Column(INTEGER, primary_key=False)
    importe = Column(DECIMAL, primary_key=False)
    concepto = Column(VARCHAR, primary_key=False)
    comprobante_asociado_id = Column(INTEGER, primary_key=False)
    asiento_id = Column(INTEGER, primary_key=False)
    user_id = Column(INTEGER, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class ErpTercerosCondiciones(Base):
    __tablename__ = 'erp_terceros_condiciones'

    enterprise_id = Column(INTEGER, primary_key=True)
    tercero_id = Column(INTEGER, primary_key=True)
    condicion_pago_id = Column(INTEGER, primary_key=True)
    fecha_habilitacion = Column(TIMESTAMP, primary_key=False)
    habilitado = Column(TINYINT, primary_key=False)

class FinCondicionesPago(Base):
    __tablename__ = 'fin_condiciones_pago'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    dias_vencimiento = Column(INTEGER, primary_key=False)
    descuento_pct = Column(DECIMAL, primary_key=False)
    recargo_pct = Column(DECIMAL, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    updated_at = Column(TIMESTAMP, primary_key=False)

class FinCondicionesPagoMixtasDetalle(Base):
    __tablename__ = 'fin_condiciones_pago_mixtas_detalle'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    mixta_id = Column(INTEGER, primary_key=False)
    condicion_pago_id = Column(INTEGER, primary_key=False)
    porcentaje = Column(DECIMAL, primary_key=False)

class FinFacturaCobros(Base):
    __tablename__ = 'fin_factura_cobros'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    factura_id = Column(INTEGER, primary_key=False)
    medio_pago_id = Column(INTEGER, primary_key=False)
    importe = Column(DECIMAL, primary_key=False)
    fecha = Column(DATETIME, primary_key=False)

class FinLiquidaciones(Base):
    __tablename__ = 'fin_liquidaciones'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    nomina_id = Column(INTEGER, primary_key=False)
    usuario_id = Column(INTEGER, primary_key=False)
    sueldo_bruto = Column(DECIMAL, primary_key=False)
    retenciones = Column(DECIMAL, primary_key=False)
    neto_a_cobrar = Column(DECIMAL, primary_key=False)

class FinNominas(Base):
    __tablename__ = 'fin_nominas'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    periodo = Column(VARCHAR, primary_key=False)
    descripcion = Column(VARCHAR, primary_key=False)
    estado = Column(VARCHAR, primary_key=False)
    total_bruto = Column(DECIMAL, primary_key=False)
    total_neto = Column(DECIMAL, primary_key=False)
    asiento_id = Column(INTEGER, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class FinMediosPago(Base):
    __tablename__ = 'fin_medios_pago'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    tipo = Column(ENUM, primary_key=False)
    cuenta_contable_id = Column(INTEGER, primary_key=False)
    recargo_pct = Column(DECIMAL, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    requiere_datos = Column(LONGTEXT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    updated_at = Column(TIMESTAMP, primary_key=False)

class FinOrdenesPago(Base):
    __tablename__ = 'fin_ordenes_pago'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    punto_venta = Column(INTEGER, primary_key=False)
    numero = Column(INTEGER, primary_key=False)
    fecha = Column(DATE, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    modulo = Column(VARCHAR, primary_key=False)
    importe_total = Column(DECIMAL, primary_key=False)
    importe_retenciones = Column(DECIMAL, primary_key=False)
    estado = Column(VARCHAR, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    user_id = Column(INTEGER, primary_key=False)

class FinOrdenesPagoComprobantes(Base):
    __tablename__ = 'fin_ordenes_pago_comprobantes'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    orden_pago_id = Column(INTEGER, primary_key=False)
    comprobante_id = Column(INTEGER, primary_key=False)
    importe_pagado = Column(DECIMAL, primary_key=False)

class FinOrdenesPagoMedios(Base):
    __tablename__ = 'fin_ordenes_pago_medios'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    orden_pago_id = Column(INTEGER, primary_key=False)
    medio_pago_id = Column(INTEGER, primary_key=False)
    importe = Column(DECIMAL, primary_key=False)
    referencia = Column(VARCHAR, primary_key=False)

class FinRetencionesEmitidas(Base):
    __tablename__ = 'fin_retenciones_emitidas'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    comprobante_pago_id = Column(INTEGER, primary_key=False)
    tipo_retencion = Column(VARCHAR, primary_key=False)
    jurisdiccion_id = Column(INTEGER, primary_key=False)
    numero_certificado = Column(VARCHAR, primary_key=False)
    fecha = Column(DATE, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    base_imponible = Column(DECIMAL, primary_key=False)
    alicuota = Column(DECIMAL, primary_key=False)
    importe_retencion = Column(DECIMAL, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class HistorialPrestamos(Base):
    __tablename__ = 'historial_prestamos'

    id = Column(INTEGER, primary_key=True)
    usuario_id = Column(INTEGER, primary_key=False)
    libro_id = Column(INTEGER, primary_key=False)
    fecha_prestamo = Column(VARCHAR, primary_key=False)
    fecha_devol_esperada = Column(VARCHAR, primary_key=False)
    fecha_devol_real = Column(VARCHAR, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class LegacyLibros(Base):
    __tablename__ = 'legacy_libros'

    id = Column(INTEGER, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    autor = Column(VARCHAR, primary_key=False)
    genero = Column(VARCHAR, primary_key=False)
    isbn = Column(VARCHAR, primary_key=False)
    precio = Column(DECIMAL, primary_key=False)
    fecha_publicacion = Column(VARCHAR, primary_key=False)
    editorial = Column(VARCHAR, primary_key=False)
    numero_paginas = Column(INTEGER, primary_key=False)
    numero_ejemplares = Column(INTEGER, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)
    stock_minimo = Column(INTEGER, primary_key=False)
    origen = Column(VARCHAR, primary_key=False)
    lengua = Column(VARCHAR, primary_key=False)
    api_checked = Column(TINYINT, primary_key=False)

class LibrosDetalles(Base):
    __tablename__ = 'libros_detalles'

    id = Column(INTEGER, primary_key=True)
    libro_id = Column(INTEGER, primary_key=False)
    descripcion = Column(TEXT, primary_key=False)
    resumen_tematico = Column(TEXT, primary_key=False)
    categorias = Column(TEXT, primary_key=False)
    idioma = Column(VARCHAR, primary_key=False)
    portada_url = Column(VARCHAR, primary_key=False)
    preview_link = Column(VARCHAR, primary_key=False)
    fecha_consulta = Column(DATETIME, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class MovimientosPendientes(Base):
    __tablename__ = 'movimientos_pendientes'

    id = Column(INTEGER, primary_key=True)
    libro_id = Column(INTEGER, primary_key=False)
    tipo = Column(ENUM, primary_key=False)
    cantidad = Column(INTEGER, primary_key=False)
    fecha_estimada = Column(DATE, primary_key=False)
    fecha_registro = Column(DATETIME, primary_key=False)
    comentario = Column(TEXT, primary_key=False)
    estado = Column(ENUM, primary_key=False)
    motivo_id = Column(INTEGER, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class StockMotivos(Base):
    __tablename__ = 'stock_motivos'

    id = Column(INTEGER, primary_key=True)
    descripcion = Column(VARCHAR, primary_key=False)
    tipo = Column(ENUM, primary_key=False)
    es_pendiente = Column(TINYINT, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)
    system_code = Column(VARCHAR, primary_key=False)

class MovimientosStock(Base):
    __tablename__ = 'movimientos_stock'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    tipo_movimiento = Column(ENUM, primary_key=False)
    cantidad = Column(DECIMAL, primary_key=False)
    fecha_movimiento = Column(TIMESTAMP, primary_key=False)
    usuario_id = Column(INTEGER, primary_key=False)
    documento_tipo = Column(VARCHAR, primary_key=False)
    documento_id = Column(INTEGER, primary_key=False)
    observaciones = Column(TEXT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class Prestamos(Base):
    __tablename__ = 'prestamos'

    id = Column(INTEGER, primary_key=True)
    usuario_id = Column(INTEGER, primary_key=False)
    libro_id = Column(INTEGER, primary_key=False)
    fecha_prestamo = Column(VARCHAR, primary_key=False)
    fecha_devol_esperada = Column(VARCHAR, primary_key=False)
    fecha_devolucion_real = Column(DATE, primary_key=False)
    estado = Column(VARCHAR, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)
    deuda = Column(DECIMAL, primary_key=False)

class Usuarios(Base):
    __tablename__ = 'usuarios'

    id = Column(INTEGER, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    apellido = Column(VARCHAR, primary_key=False)
    telefono = Column(VARCHAR, primary_key=False)
    email = Column(VARCHAR, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class ServiceEfficiency(Base):
    __tablename__ = 'service_efficiency'

    service_name = Column(VARCHAR, primary_key=True)
    hits_count = Column(INTEGER, primary_key=False)
    fields_provided = Column(INTEGER, primary_key=False)
    last_updated = Column(TIMESTAMP, primary_key=False)
    ebooks_provided = Column(INTEGER, primary_key=False)

class StkArchivosDigitales(Base):
    __tablename__ = 'stk_archivos_digitales'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    contenido = Column(MEDIUMBLOB, primary_key=False)
    formato = Column(VARCHAR, primary_key=False)
    nombre_archivo = Column(VARCHAR, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class StkDepositos(Base):
    __tablename__ = 'stk_depositos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    tipo = Column(ENUM, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)
    direccion = Column(VARCHAR, primary_key=False)
    calle = Column(VARCHAR, primary_key=False)
    numero = Column(VARCHAR, primary_key=False)
    localidad = Column(VARCHAR, primary_key=False)
    provincia = Column(VARCHAR, primary_key=False)
    municipio = Column(VARCHAR, primary_key=False)
    cod_postal = Column(VARCHAR, primary_key=False)
    es_principal = Column(TINYINT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(DATETIME, primary_key=False)

class StkExistencias(Base):
    __tablename__ = 'stk_existencias'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    deposito_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    cantidad = Column(INTEGER, primary_key=False)
    ubicacion = Column(VARCHAR, primary_key=False)
    last_updated = Column(DATETIME, primary_key=False)

class StkInventarios(Base):
    __tablename__ = 'stk_inventarios'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    deposito_id = Column(INTEGER, primary_key=False)
    fecha_inicio = Column(DATETIME, primary_key=False)
    fecha_cierre = Column(DATETIME, primary_key=False)
    tipo = Column(ENUM, primary_key=False)
    estado = Column(ENUM, primary_key=False)
    responsable_id = Column(INTEGER, primary_key=False)
    observaciones = Column(TEXT, primary_key=False)
    criteria_json = Column(TEXT, primary_key=False)

class StkItemsInventario(Base):
    __tablename__ = 'stk_items_inventario'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    inventario_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    stock_sistema = Column(DECIMAL, primary_key=False)
    stock_fisico = Column(DECIMAL, primary_key=False)
    diferencia = Column(DECIMAL, primary_key=False)
    ajustado = Column(TINYINT, primary_key=False)

class StkItemsTransferencia(Base):
    __tablename__ = 'stk_items_transferencia'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    transferencia_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    cantidad = Column(DECIMAL, primary_key=False)

class StkTransferencias(Base):
    __tablename__ = 'stk_transferencias'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    origen_id = Column(INTEGER, primary_key=False)
    destino_id = Column(INTEGER, primary_key=False)
    destino_final_direccion = Column(VARCHAR, primary_key=False)
    logistica_id = Column(INTEGER, primary_key=False)
    tipo_transporte = Column(ENUM, primary_key=False)
    fecha = Column(DATETIME, primary_key=False)
    estado = Column(ENUM, primary_key=False)
    cot_numero = Column(VARCHAR, primary_key=False)
    motivo = Column(VARCHAR, primary_key=False)
    usuario_id = Column(INTEGER, primary_key=False)

class StkLogisticas(Base):
    __tablename__ = 'stk_logisticas'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    cuit = Column(VARCHAR, primary_key=False)
    direccion = Column(VARCHAR, primary_key=False)
    calle = Column(VARCHAR, primary_key=False)
    numero = Column(VARCHAR, primary_key=False)
    localidad = Column(VARCHAR, primary_key=False)
    provincia = Column(VARCHAR, primary_key=False)
    email = Column(VARCHAR, primary_key=False)
    telefono = Column(VARCHAR, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class StkMotivos(Base):
    __tablename__ = 'stk_motivos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    tipo = Column(VARCHAR, primary_key=False)
    automatico = Column(TINYINT, primary_key=False)
    created_at = Column(DATETIME, primary_key=False)
    es_pendiente = Column(TINYINT, primary_key=False)
    system_code = Column(VARCHAR, primary_key=False)

class StkMovimientos(Base):
    __tablename__ = 'stk_movimientos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    fecha = Column(DATETIME, primary_key=False)
    motivo_id = Column(INTEGER, primary_key=False)
    deposito_origen_id = Column(INTEGER, primary_key=False)
    deposito_destino_id = Column(INTEGER, primary_key=False)
    comprobante_id = Column(INTEGER, primary_key=False)
    user_id = Column(INTEGER, primary_key=False)
    observaciones = Column(TEXT, primary_key=False)
    estado = Column(VARCHAR, primary_key=False)
    tercero_id = Column(INTEGER, primary_key=False)

class StkMovimientosDetalle(Base):
    __tablename__ = 'stk_movimientos_detalle'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    movimiento_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    cantidad = Column(INTEGER, primary_key=False)
    created_at = Column(DATETIME, primary_key=False)

class StkServiciosConfig(Base):
    __tablename__ = 'stk_servicios_config'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    articulo_id = Column(INTEGER, primary_key=False)
    frecuencia_facturacion = Column(ENUM, primary_key=False)
    dia_facturacion = Column(INTEGER, primary_key=False)
    mes_facturacion = Column(INTEGER, primary_key=False)
    prorrateable = Column(TINYINT, primary_key=False)
    dias_vencimiento = Column(INTEGER, primary_key=False)
    genera_deuda_automatica = Column(TINYINT, primary_key=False)

class StkTiposArticulo(Base):
    __tablename__ = 'stk_tipos_articulo'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    descripcion = Column(VARCHAR, primary_key=False)
    naturaleza = Column(VARCHAR, primary_key=False)
    usa_api_libros = Column(TINYINT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    custom_fields_schema = Column(TEXT, primary_key=False)

class StkTiposArticuloServicios(Base):
    __tablename__ = 'stk_tipos_articulo_servicios'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    tipo_articulo_id = Column(INTEGER, primary_key=False)
    servicio_id = Column(INTEGER, primary_key=False)
    config_overwrite_json = Column(LONGTEXT, primary_key=False)
    es_primario = Column(TINYINT, primary_key=False)

class SysExternalServices(Base):
    __tablename__ = 'sys_external_services'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    tipo_servicio = Column(VARCHAR, primary_key=False)
    clase_implementacion = Column(VARCHAR, primary_key=False)
    config_json = Column(LONGTEXT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)
    modo_captura = Column(VARCHAR, primary_key=False)
    url_objetivo = Column(VARCHAR, primary_key=False)
    system_code = Column(VARCHAR, primary_key=False)
    last_status = Column(VARCHAR, primary_key=False)

class StkTiposArticulos(Base):
    __tablename__ = 'stk_tipos_articulos'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    descripcion = Column(TEXT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    custom_fields_schema = Column(TEXT, primary_key=False)

class StockAjustes(Base):
    __tablename__ = 'stock_ajustes'

    id = Column(INTEGER, primary_key=True)
    libro_id = Column(INTEGER, primary_key=False)
    motivo_id = Column(INTEGER, primary_key=False)
    cantidad = Column(INTEGER, primary_key=False)
    fecha = Column(DATETIME, primary_key=False)
    comentario = Column(TEXT, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class SysActiveTasks(Base):
    __tablename__ = 'sys_active_tasks'

    task_id = Column(VARCHAR, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    process_name = Column(VARCHAR, primary_key=False)
    description = Column(TEXT, primary_key=False)
    priority = Column(INTEGER, primary_key=False)
    parent_id = Column(VARCHAR, primary_key=False)
    thread_id = Column(BIGINT, primary_key=False)
    start_time = Column(DOUBLE, primary_key=False)
    last_heartbeat = Column(TIMESTAMP, primary_key=False)
    os_pid = Column(INTEGER, primary_key=False)
    source_type = Column(VARCHAR, primary_key=False)
    status = Column(VARCHAR, primary_key=False)
    requested_stop = Column(TINYINT, primary_key=False)
    source_origin = Column(VARCHAR, primary_key=False)

class SysConfigFiscal(Base):
    __tablename__ = 'sys_config_fiscal'

    enterprise_id = Column(INTEGER, primary_key=True)
    nro_agente_arba = Column(VARCHAR, primary_key=False)
    nro_agente_agip = Column(VARCHAR, primary_key=False)
    nro_agente_municipality = Column(VARCHAR, primary_key=False)
    leyenda_certificados = Column(TEXT, primary_key=False)
    firma_digital_id = Column(INTEGER, primary_key=False)

class SysCrons(Base):
    __tablename__ = 'sys_crons'

    id = Column(INTEGER, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    descripcion = Column(TEXT, primary_key=False)
    comando = Column(VARCHAR, primary_key=False)
    frecuencia = Column(VARCHAR, primary_key=False)
    planificacion = Column(LONGTEXT, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=False)
    ultima_ejecucion = Column(DATETIME, primary_key=False)
    proxima_ejecucion = Column(DATETIME, primary_key=False)
    estado = Column(ENUM, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class SysCronsLogs(Base):
    __tablename__ = 'sys_crons_logs'

    id = Column(INTEGER, primary_key=True)
    cron_id = Column(INTEGER, primary_key=False)
    fecha_inicio = Column(DATETIME, primary_key=False)
    fecha_fin = Column(DATETIME, primary_key=False)
    status = Column(ENUM, primary_key=False)
    resultado = Column(TEXT, primary_key=False)

class SysDepartamentos(Base):
    __tablename__ = 'sys_departamentos'

    id = Column(VARCHAR, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    provincia_id = Column(VARCHAR, primary_key=False)
    centroide_lat = Column(DECIMAL, primary_key=False)
    centroide_lon = Column(DECIMAL, primary_key=False)

class SysProvincias(Base):
    __tablename__ = 'sys_provincias'

    id = Column(VARCHAR, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    codigo_jurisdiccion = Column(INTEGER, primary_key=False)
    iso_id = Column(VARCHAR, primary_key=False)
    centroide_lat = Column(DECIMAL, primary_key=False)
    centroide_lon = Column(DECIMAL, primary_key=False)

class SysEnrichmentCounters(Base):
    __tablename__ = 'sys_enrichment_counters'

    id = Column(TINYINT, primary_key=True)
    processed_since_reset = Column(INTEGER, primary_key=False)

class SysEnterpriseLogos(Base):
    __tablename__ = 'sys_enterprise_logos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    logo_data = Column(LONGBLOB, primary_key=False)
    mime_type = Column(VARCHAR, primary_key=False)
    is_active = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class SysEnterprisesFiscal(Base):
    __tablename__ = 'sys_enterprises_fiscal'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    jurisdiccion = Column(VARCHAR, primary_key=False)
    tipo = Column(ENUM, primary_key=False)
    fecha_notificacion = Column(DATE, primary_key=False)
    nro_notificacion = Column(VARCHAR, primary_key=False)
    activo = Column(TINYINT, primary_key=False)

class SysEnterprisesNew(Base):
    __tablename__ = 'sys_enterprises_new'

    id = Column(INTEGER, primary_key=True)
    codigo = Column(VARCHAR, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    estado = Column(VARCHAR, primary_key=False)

class SysImpuestos(Base):
    __tablename__ = 'sys_impuestos'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    descripcion = Column(VARCHAR, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    created_at = Column(TIMESTAMP, primary_key=False)

class SysJurisdiccionesIibb(Base):
    __tablename__ = 'sys_jurisdicciones_iibb'

    codigo = Column(INTEGER, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    alias = Column(VARCHAR, primary_key=False)
    activo = Column(TINYINT, primary_key=False)

class SysLocalidades(Base):
    __tablename__ = 'sys_localidades'

    id = Column(VARCHAR, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    provincia_id = Column(VARCHAR, primary_key=False)
    municipio_id = Column(VARCHAR, primary_key=False)
    centroide_lat = Column(DECIMAL, primary_key=False)
    centroide_lon = Column(DECIMAL, primary_key=False)

class SysMunicipios(Base):
    __tablename__ = 'sys_municipios'

    id = Column(VARCHAR, primary_key=True)
    nombre = Column(VARCHAR, primary_key=False)
    nombre_completo = Column(VARCHAR, primary_key=False)
    provincia_id = Column(VARCHAR, primary_key=False)
    centroide_lat = Column(DECIMAL, primary_key=False)
    centroide_lon = Column(DECIMAL, primary_key=False)
    categoria = Column(VARCHAR, primary_key=False)

class SysPadronesIibb(Base):
    __tablename__ = 'sys_padrones_iibb'

    id = Column(INTEGER, primary_key=True)
    jurisdiccion_id = Column(INTEGER, primary_key=False)
    jurisdiccion = Column(VARCHAR, primary_key=False)
    cuit = Column(VARCHAR, primary_key=False)
    tipo_contribuyente = Column(VARCHAR, primary_key=False)
    alicuota_percepcion = Column(DECIMAL, primary_key=False)
    alicuota_retencion = Column(DECIMAL, primary_key=False)
    grupo_riesgo = Column(INTEGER, primary_key=False)
    desde = Column(DATE, primary_key=False)
    hasta = Column(DATE, primary_key=False)
    exencion_iibb = Column(VARCHAR, primary_key=False)

class SysPadronesLogs(Base):
    __tablename__ = 'sys_padrones_logs'

    id = Column(INTEGER, primary_key=True)
    jurisdiccion = Column(VARCHAR, primary_key=False)
    fecha_ejecucion = Column(DATETIME, primary_key=False)
    tipo_proceso = Column(VARCHAR, primary_key=False)
    archivo_origen = Column(VARCHAR, primary_key=False)
    registros_procesados = Column(INTEGER, primary_key=False)
    registros_altas = Column(INTEGER, primary_key=False)
    registros_bajas = Column(INTEGER, primary_key=False)
    registros_modificaciones = Column(INTEGER, primary_key=False)
    status = Column(VARCHAR, primary_key=False)
    mensaje = Column(TEXT, primary_key=False)

class SysPermissions(Base):
    __tablename__ = 'sys_permissions'

    id = Column(INTEGER, primary_key=True)
    code = Column(VARCHAR, primary_key=False)
    description = Column(VARCHAR, primary_key=False)
    category = Column(VARCHAR, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class SysRolePermissions(Base):
    __tablename__ = 'sys_role_permissions'

    role_id = Column(INTEGER, primary_key=True)
    permission_id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=True)

class SysRoles(Base):
    __tablename__ = 'sys_roles'

    id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR, primary_key=False)
    description = Column(TEXT, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class SysSecurityLogs(Base):
    __tablename__ = 'sys_security_logs'

    id = Column(INTEGER, primary_key=True)
    event_time = Column(DATETIME, primary_key=False)
    actor_user_id = Column(INTEGER, primary_key=False)
    target_user_id = Column(INTEGER, primary_key=False)
    action = Column(VARCHAR, primary_key=False)
    status = Column(VARCHAR, primary_key=False)
    details = Column(TEXT, primary_key=False)
    ip_address = Column(VARCHAR, primary_key=False)
    session_id = Column(VARCHAR, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)

class SysUsers(Base):
    __tablename__ = 'sys_users'

    id = Column(INTEGER, primary_key=True)
    username = Column(VARCHAR, primary_key=False)
    password_hash = Column(VARCHAR, primary_key=False)
    email = Column(VARCHAR, primary_key=False)
    role_id = Column(INTEGER, primary_key=False)
    created_at = Column(DATETIME, primary_key=False)
    temp_password_hash = Column(VARCHAR, primary_key=False)
    temp_password_expires = Column(DATETIME, primary_key=False)
    recovery_attempts = Column(INTEGER, primary_key=False)
    temp_password_used = Column(TINYINT, primary_key=False)
    updated_at = Column(TIMESTAMP, primary_key=False)
    enterprise_id = Column(INTEGER, primary_key=True)
    last_login_at = Column(DATETIME, primary_key=False)
    is_active = Column(TINYINT, primary_key=False)
    created_by = Column(INTEGER, primary_key=False)
    updated_by = Column(INTEGER, primary_key=False)

class SysTiposComprobante(Base):
    __tablename__ = 'sys_tipos_comprobante'

    enterprise_id = Column(INTEGER, primary_key=True)
    codigo = Column(VARCHAR, primary_key=True)
    descripcion = Column(VARCHAR, primary_key=False)
    letra = Column(VARCHAR, primary_key=False)

class SystemStats(Base):
    __tablename__ = 'system_stats'

    key_name = Column(VARCHAR, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=True)
    value_int = Column(INTEGER, primary_key=False)
    last_updated = Column(TIMESTAMP, primary_key=False)
    value_str = Column(TEXT, primary_key=False)

class TaxAlicuotas(Base):
    __tablename__ = 'tax_alicuotas'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    impuesto_id = Column(INTEGER, primary_key=False)
    alicuota = Column(DECIMAL, primary_key=False)
    base_calculo = Column(ENUM, primary_key=False)
    vigencia_desde = Column(DATE, primary_key=False)
    vigencia_hasta = Column(DATE, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    observaciones = Column(VARCHAR, primary_key=False)
    updated_at = Column(DATETIME, primary_key=False)

class TaxImpuestos(Base):
    __tablename__ = 'tax_impuestos'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    codigo = Column(VARCHAR, primary_key=False)
    nombre = Column(VARCHAR, primary_key=False)
    tipo = Column(ENUM, primary_key=False)
    descripcion = Column(TEXT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)
    orden_display = Column(INTEGER, primary_key=False)
    created_at = Column(DATETIME, primary_key=False)

class TaxEngineSnapshots(Base):
    __tablename__ = 'tax_engine_snapshots'

    enterprise_id = Column(INTEGER, primary_key=False)
    id = Column(INTEGER, primary_key=True)
    version_id = Column(INTEGER, primary_key=False)
    reglas_json = Column(LONGTEXT, primary_key=False)
    alicuotas_json = Column(LONGTEXT, primary_key=False)
    created_at = Column(DATETIME, primary_key=False)

class TaxEngineVersions(Base):
    __tablename__ = 'tax_engine_versions'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    version_code = Column(VARCHAR, primary_key=False)
    fecha_implementacion = Column(DATETIME, primary_key=False)
    usuario_id = Column(INTEGER, primary_key=False)
    descripcion = Column(TEXT, primary_key=False)

class TaxReglas(Base):
    __tablename__ = 'tax_reglas'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    operacion = Column(ENUM, primary_key=False)
    tipo_responsable = Column(VARCHAR, primary_key=False)
    condicion_iibb = Column(VARCHAR, primary_key=False)
    exencion_iibb = Column(VARCHAR, primary_key=False)
    impuesto_id = Column(INTEGER, primary_key=False)
    aplica = Column(TINYINT, primary_key=False)
    es_obligatorio = Column(TINYINT, primary_key=False)
    activo = Column(TINYINT, primary_key=False)

class TaxReglasIibb(Base):
    __tablename__ = 'tax_reglas_iibb'

    id = Column(INTEGER, primary_key=True)
    enterprise_id = Column(INTEGER, primary_key=False)
    condicion_iibb = Column(VARCHAR, primary_key=False)
    jurisdiccion_codigo = Column(INTEGER, primary_key=False)
    jurisdiccion_nombre = Column(VARCHAR, primary_key=False)
    impuesto_id = Column(INTEGER, primary_key=False)
    alicuota_override = Column(DECIMAL, primary_key=False)
    usa_padron = Column(TINYINT, primary_key=False)
    regimen = Column(ENUM, primary_key=False)
    limite_cm_pct = Column(DECIMAL, primary_key=False)
    coef_minimo_cm = Column(DECIMAL, primary_key=False)
    activo = Column(TINYINT, primary_key=False)

