-- ============================================================
-- SEED CONTABLE MSAC v4.0 — Cuentas para todos los módulos
-- Compras / Ventas / Stock / Costos Industriales / Consignación
-- Basado en plan de cuentas argentino estándar (FACPCE)
-- ============================================================
-- NOTA: Usa INSERT IGNORE para no duplicar si ya existen.
-- Ejecutar una vez por empresa, pasando el @ENT_ID correspondiente.
-- SET @ENT_ID = 1; -- Ajustar a la empresa destino
-- ============================================================
-- CLASE 1: ACTIVO
-- ============================================================
-- 1.1 Activo Corriente - Disponibilidades
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '1.1',
        'Activo Corriente',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.1.01',
        'Caja',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.1.02',
        'Bancos',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.1.03',
        'Cheques de Terceros en Cartera',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.1.04',
        'Billeteras Virtuales',
        'DETALLE',
        'DEUDORA',
        1
    );
-- 1.2 Activo Corriente - Créditos
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '1.2',
        'Créditos',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.2.01',
        'IVA Crédito Fiscal (Compras)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.2.02',
        'Retenciones de IIBB a Recuperar',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.2.03',
        'Anticipos a Proveedores',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.2.04',
        'Saldos a Favor AFIP (Ganancias)',
        'DETALLE',
        'DEUDORA',
        1
    );
-- 1.3 Activo Corriente - Clientes y Deudores
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '1.3',
        'Deudores por Ventas',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.3.01',
        'Deudores por Ventas (Clientes)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.3.02',
        'Documentos a Cobrar',
        'DETALLE',
        'DEUDORA',
        1
    );
-- 1.4 Activo Corriente - Bienes de Cambio (Stock)
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '1.4',
        'Bienes de Cambio',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.01',
        'Mercaderías de Reventa (Stock)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.02',
        'Materias Primas',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.03',
        'Productos en Proceso (WIP)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.04',
        'Productos Terminados',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.05',
        'Mercaderías en Tránsito - Importación (FOB + Flete + Seguro)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.05.01',
        'Costo FOB de la Mercadería Importada',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.05.02',
        'Flete Internacional (en Tránsito)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.05.03',
        'Seguro Internacional (en Tránsito)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.06',
        'Mercaderías en Consignación (Entregadas en Tenencia)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.4.07',
        'Mercaderías Recibidas en Consignación (Pasivo Contingente)',
        'DETALLE',
        'DEUDORA',
        1
    );
-- 1.5 Activo No Corriente - Bienes de Uso (Amortizaciones)
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '1.5',
        'Bienes de Uso',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.5.01',
        'Maquinaria y Equipos',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.5.02',
        'Amortización Acumulada Maquinaria',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '1.5.03',
        'Instalaciones y Mejoras',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.5.04',
        'Amortización Acumulada Instalaciones',
        'DETALLE',
        'ACREEDORA',
        1
    );
-- ============================================================
-- 1.2 AMPLIACION: Créditos por Importacion
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '1.2.05',
        'IVA Crédito Fiscal por Importación de Servicios',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.2.06',
        'Anticipos y Garantías de Importación (BCRA)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.2.07',
        'Percepciones AFIP por Importación a Recuperar',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- CLASE 2: PASIVO
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '2.1',
        'Deudas Comerciales',
        'RUBRO',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.01',
        'Proveedores (Deudas por Compras)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.02',
        'Documentos a Pagar',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.03',
        'Anticipo de Clientes',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.05',
        'Proveedores del Exterior (USD / EUR)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.06',
        'Derechos de Importación a Pagar (DGA)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.07',
        'Tasa de Estadística a Pagar (DGA)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.08',
        'Gastos de Despachante de Aduana a Pagar',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.09',
        'Gastos Portuarios y Almacenaje a Pagar',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.1.04',
        'Responsabilidad por Consignaciones Recibidas',
        'DETALLE',
        'ACREEDORA',
        1
    );
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '2.2',
        'Deudas Fiscales',
        'RUBRO',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.2.01',
        'IVA Débito Fiscal (Ventas)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.2.02',
        'Retenciones Ganancias a Depositar',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.2.03',
        'Percepciones IIBB a Depositar',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.2.04',
        'Retenciones IIBB a Depositar',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '2.2.05',
        'Aportes y Contribuciones a Pagar (RRHH)',
        'DETALLE',
        'ACREEDORA',
        1
    );
-- ============================================================
-- CLASE 4: VENTAS E INGRESOS
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '4',
        'Ingresos',
        'CLASE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '4.1',
        'Ventas de Mercaderías',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '4.2',
        'Ventas de Productos Propios (Producción)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '4.3',
        'Ingresos por Servicios de Fazón',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '4.4',
        'Descuentos y Bonificaciones a Clientes',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- CLASE 5: COSTOS
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '5',
        'Costos',
        'CLASE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.1',
        'Costo de Mercaderías Vendidas (CMV)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.2',
        'Costo de Producción (Industrial)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.3',
        'Mano de Obra Directa',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.4',
        'Gastos de Fabricación (Overhead)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.4.01',
        'Energía y Servicios Industriales',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.4.02',
        'Amortización Maquinaria Industrial',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.4.03',
        'Fletes y Acarreos Internos',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.4.04',
        'Mantenimiento y Reparaciones',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.5',
        'Variación de Inventarios (WIP)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.6',
        'Mermas y Pérdidas en Producción',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- CLASE 6: GASTOS OPERATIVOS
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '6',
        'Gastos Operativos',
        'CLASE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.1',
        'Gastos de Comercialización',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.1.01',
        'Comisiones de Vendedores',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.1.02',
        'Fletes y Despacho a Clientes',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.2',
        'Gastos de Administración',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.2.01',
        'Sueldos y Jornales (RRHH)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.2.02',
        'Cargas Sociales',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.2.03',
        'Alquileres',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.2.04',
        'Servicios (Luz, Gas, Internet)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.2.05',
        'Amortizaciones Bienes de Uso',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.3',
        'Gastos Financieros',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.3.01',
        'Intereses y Gastos Bancarios',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.3.02',
        'Diferencias de Cambio',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- 5.X AMPLIACION: Gastos de Importación capitalizables al costo
-- Son los gastos de ARRIBO que forman parte del costo de la mercadería
-- conforme al marco contable FACPCE (RT 17) y Código Aduanero
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '5.7',
        'Gastos de Importación (Costo de Arribo)',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.01',
        'Derechos de Importación (Ad Valorem)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.02',
        'Tasa de Estadística',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.03',
        'Flete Internacional (Airfreight/Seafreight)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.04',
        'Seguro Internacional (CIF)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.05',
        'Honorarios Despachante de Aduana',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.06',
        'Gastos Portuarios y Almacenaje (Terminal)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.07',
        'Fletes Internos desde Puerto/Aduana',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '5.7.08',
        'Percepción AFIP por Importación (IVA Imp)',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- 6.3 AMPLIACION: Resultados Financieros por Importacion
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '6.3.03',
        'Diferencia de Cambio Favorable (Importacion)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '6.3.04',
        'Diferencia de Cambio Desfavorable (Importacion)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.3.05',
        'Gastos Bancarios por Transferencias Exterior (SWIFT)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.3.06',
        'Costo de Acceso al MULC (Mercado Único de Cambios)',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- CLASE 1: ACTIVOS RECUPERABLES (SCRAP con valor residual)
-- Cuando el scrap tiene valor de reventa (chatarra, retazos, rezagos)
-- se activa a su valor neto de realizacion, no va a perdida directa
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '1.6',
        'Otros Activos Corrientes',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.6.01',
        'Scrap con Valor de Recupero (Chatarra / Retazos)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.6.02',
        'Rezagos de Importacion a Liquidar',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '1.6.03',
        'Subproductos de Produccion Vendibles',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- CLASE 4: INGRESOS POR SCRAP
-- Cuando el scrap se vende o tiene valor de recupero
-- Contrapartida del activo 1.6.01 al momento de venta
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '4.5',
        'Ingresos por Venta de Scrap / Chatarra',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '4.6',
        'Recupero por Venta de Subproductos',
        'DETALLE',
        'ACREEDORA',
        1
    );
-- ============================================================
-- CLASE 6.4: SANCIONES, PENALIDADES Y GASTOS EXTRAORDINARIOS
-- Demurrage: cargo por exceso de estadía de contenedores
-- NO capitalizable al costo. Va a resultado del periodo.
-- Multas DGA/AFIP: sanciones formales e infracciones aduaneras
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '6.4',
        'Penalidades, Sanciones y Gastos Extraordinarios',
        'RUBRO',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.4.01',
        'Demurrage (Estadia Excesiva de Contenedores)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.4.02',
        'Detention (Demora en Devolucion de Contenedor)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.4.03',
        'Multas Aduaneras - DGA',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.4.04',
        'Multas AFIP (Formales e Impositivas)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.4.05',
        'Multas por Incumplimiento Contractual a Proveedores',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.4.06',
        'Recargos e Intereses por Mora (Impuestos)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '6.4.07',
        'Costas y Gastos Judiciales',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- CLASE 7: RESULTADOS EXTRAORDINARIOS Y SCRAP SIN RECUPERO
-- Diferencia con 6.4: estos son eventos NO recurrentes
-- Scrap sin valor residual: perdida directa a resultado
-- FACPCE RT17: el scrap se mide a VNR (Valor Neto de Realizacion)
-- Si VNR = 0 => perdida por el costo asignado en el BOM
-- Si VNR > 0 => se activa en 1.6.01 y se reduce el CMV
-- ============================================================
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        @ENT_ID,
        '7',
        'Resultados Extraordinarios',
        'CLASE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '7.1',
        'Perdidas por Mermas Extraordinarias (Scrap sin VNR)',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '7.1.01',
        'Scrap de Produccion sin Valor Residual',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '7.1.02',
        'Merma por Siniestro o Accidente Industrial',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '7.1.03',
        'Perdida por Vencimiento / Obsolescencia de Stock',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '7.1.04',
        'Perdida por Robo o Faltante Confirmado de Inventario',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        @ENT_ID,
        '7.2',
        'Recuperos y Resultados Extraordinarios Positivos',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '7.2.01',
        'Recupero de Seguros por Siniestro',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '7.2.02',
        'Ajuste de Inventario Favorable (Sobrante)',
        'DETALLE',
        'ACREEDORA',
        1
    ),
    (
        @ENT_ID,
        '7.3',
        'Provision para Litigios y Contingencias',
        'DETALLE',
        'DEUDORA',
        1
    );
-- ============================================================
-- TABLA DE MAPEO: Tipo de gasto -> Cuenta Contable (Fase 1.3)
-- Vincula los ENUM de overhead con el plan de cuentas
-- ============================================================
CREATE TABLE IF NOT EXISTS cmp_overhead_cuenta_contable (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    tipo_gasto ENUM(
        'MANO_DE_OBRA',
        'ENERGIA',
        'AMORTIZACION',
        'FLETE_INTERNO',
        'OTROS'
    ) NOT NULL,
    cuenta_codigo VARCHAR(20) NOT NULL,
    activo TINYINT(1) DEFAULT 1,
    UNIQUE KEY uq_empresa_tipo (enterprise_id, tipo_gasto)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
INSERT IGNORE INTO cmp_overhead_cuenta_contable (enterprise_id, tipo_gasto, cuenta_codigo)
VALUES (@ENT_ID, 'MANO_DE_OBRA', '5.3'),
    (@ENT_ID, 'ENERGIA', '5.4.01'),
    (@ENT_ID, 'AMORTIZACION', '5.4.02'),
    (@ENT_ID, 'FLETE_INTERNO', '5.4.03'),
    (@ENT_ID, 'OTROS', '5.4');