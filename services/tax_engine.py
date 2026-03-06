"""
Tax Engine — Motor de cálculo fiscal parametrizable por empresa.

Responsabilidades:
  - Resolver qué impuestos aplican dado: operación + perfil del tercero + condición IIBB
  - Calcular importes según alícuotas vigentes (con herencia enterprise_id=0)
  - Exponer la configuración al frontend vía get_reglas_para_frontend()
  - Calcular totales de un comprobante

Herencia de reglas:
  1. Busca reglas específicas de la empresa (enterprise_id propio)
  2. Si no hay, usa las del enterprise_id=0 (plantilla global)
  3. Las alícuotas siguen el mismo patrón de herencia

Operaciones de negocio soportadas:
  COMPRAS   → Facturas de compra a proveedores
  VENTAS    → Facturas de venta a clientes
  COBRANZAS → Recibos y cobros
  PAGOS     → Órdenes de pago a proveedores

Uso básico:
    from services.tax_engine import TaxEngine

    engine = TaxEngine(enterprise_id=1)

    # Obtener reglas para el frontend (qué campos mostrar)
    reglas = engine.get_reglas_para_frontend(
        operacion='COMPRAS',
        tipo_responsable='RI',
        condicion_iibb='ARBA'
    )

    # Calcular impuestos sobre importes
    resultado = engine.calcular(
        operacion='COMPRAS',
        tipo_responsable='RI',
        condicion_iibb='ARBA',
        importes={'neto_21': 1000.0, 'neto_10_5': 500.0}
    )
"""

from database import get_db_cursor
import datetime
import json


# ─────────────────────────────────────────────────────────────────────────────
# Constantes de operaciones de negocio
# ─────────────────────────────────────────────────────────────────────────────
OPERACION_COMPRAS   = 'COMPRAS'
OPERACION_VENTAS    = 'VENTAS'
OPERACION_COBRANZAS = 'COBRANZAS'
OPERACION_PAGOS     = 'PAGOS'

OPERACIONES_VALIDAS = {OPERACION_COMPRAS, OPERACION_VENTAS, OPERACION_COBRANZAS, OPERACION_PAGOS}

# Tipos responsable que NO discriminan IVA
TIPOS_SIN_IVA = {
    'MONOTRIBUTO', 'MONOTRIBUTISTA', 'EXENTO',
    'NO_RESPONSABLE', 'CONSUMIDOR_FINAL', 'PEQUEÑO_CONTRIBUYENTE',
    'PEQUENO_CONTRIBUYENTE'
}

# Condiciones IIBB que activan Convenio Multilateral
CONDICIONES_CM = {'CONVENIO_MULTILATERAL', 'CM', 'MULTILATERAL'}


class TaxEngine:
    """
    Motor de cálculo fiscal parametrizable por empresa y operación de negocio.

    Cada instancia está ligada a un enterprise_id. Las reglas se resuelven
    con herencia: primero busca config propia, luego usa la plantilla global (id=0).
    """

    def __init__(self, enterprise_id: int):
        self.enterprise_id = enterprise_id
        self._cache_reglas = {}       # Cache por (operacion, tipo_resp, cond_iibb)
        self._cache_alicuotas = None  # Cache de alícuotas vigentes
        self._cache_iibb = {}         # Cache de reglas IIBB

    # ─────────────────────────────────────────────────────────────────────────
    # MÉTODO PRINCIPAL: reglas para el frontend
    # ─────────────────────────────────────────────────────────────────────────

    def get_reglas_para_frontend(self, operacion: str, tipo_responsable: str,
                                  condicion_iibb: str = '',
                                  exencion_iibb: str = '') -> dict:
        """
        Retorna la configuración que el frontend necesita para mostrar/ocultar
        campos y calcular impuestos.

        Args:
            operacion:        'COMPRAS', 'VENTAS', etc.
            tipo_responsable: 'RI', 'MONOTRIBUTO', etc.
            condicion_iibb:   'ARBA', 'AGIP', 'AMBOS', 'CONVENIO_MULTILATERAL', etc.
            exencion_iibb:    'EXENTO' si el padron informa exencion (ej: profesional CABA),
                              'NO_EXENTO' o '' si no hay exencion.
                              El engine resuelve la regla más específica primero.
        """
        tipo_norm    = self._normalizar_tipo(tipo_responsable)
        cond_norm    = self._normalizar_iibb(condicion_iibb)
        exencion_norm = self._normalizar_exencion(exencion_iibb)

        impuestos_aplicables = self._resolver_impuestos(operacion, tipo_norm, cond_norm, exencion_norm)
        alicuotas = self._get_alicuotas_vigentes()
        reglas_iibb = self._resolver_iibb(cond_norm)

        # Construir lista de impuestos con metadatos para el frontend
        impuestos_out = []
        tiene_iva = False

        for imp in impuestos_aplicables:
            codigo = imp['codigo']
            alicuota = alicuotas.get(codigo, {}).get('alicuota', 0.0)
            base = alicuotas.get(codigo, {}).get('base_calculo', 'NETO_GRAVADO')

            if imp['tipo'] == 'IVA':
                tiene_iva = True

            impuestos_out.append({
                'codigo':        codigo,
                'nombre':        imp['nombre'],
                'tipo':          imp['tipo'],
                'alicuota':      float(alicuota),
                'base_calculo':  base,
                'es_obligatorio': bool(imp.get('es_obligatorio', 0)),
                # Nombres de campos HTML para el formulario
                'campo_neto':     self._campo_neto(codigo),
                'campo_impuesto': self._campo_impuesto(codigo),
            })

        # Si el tipo no discrimina IVA, forzar tiene_iva=False
        if tipo_norm in TIPOS_SIN_IVA:
            tiene_iva = False
            impuestos_out = [i for i in impuestos_out if i['tipo'] != 'IVA']

        es_cm = cond_norm in CONDICIONES_CM

        return {
            'tiene_iva':    tiene_iva,
            'impuestos':    impuestos_out,
            'iibb': {
                'tipo':          cond_norm or 'NINGUNO',
                'es_cm':         es_cm,
                'jurisdicciones': reglas_iibb,
            },
            'perfil_label': (
                f"{tipo_responsable} | IIBB: {condicion_iibb or 'No inscripto'}"
                + (f" | Exento AGIP" if exencion_norm == 'EXENTO' else "")
            ),
            'operacion':    operacion,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # CÁLCULO DE IMPUESTOS
    # ─────────────────────────────────────────────────────────────────────────

    def calcular(self, operacion: str, tipo_responsable: str,
                 condicion_iibb: str, importes: dict,
                 percepciones_cm: list = None) -> dict:
        """
        Calcula los impuestos sobre los importes provistos.

        Args:
            operacion:        'COMPRAS', 'VENTAS', etc.
            tipo_responsable: 'RI', 'MONOTRIBUTO', etc.
            condicion_iibb:   'ARBA', 'AGIP', 'CONVENIO_MULTILATERAL', etc.
            importes: {
                'neto_21': 1000.0,
                'neto_10_5': 500.0,
                'neto_27': 0.0,
                'importe_total_sin_iva': 0.0,  # Para monotributo
                'importe_exento': 0.0,
                'importe_no_gravado': 0.0,
                'perc_iva': 0.0,
                'perc_arba': 0.0,
                'perc_agip': 0.0,
                'otros_imp': 0.0,
            }
            percepciones_cm: [{'jurisdiccion': '900', 'importe': 150.0}, ...]

        Returns:
            {
                'neto_total': 1500.0,
                'iva_total': 315.0,
                'percepciones_total': 45.0,
                'total': 1860.0,
                'detalle': { 'iva_21': 210.0, 'iva_10_5': 52.5, ... }
            }
        """
        tipo_norm = self._normalizar_tipo(tipo_responsable)
        alicuotas = self._get_alicuotas_vigentes()

        # IVA
        neto_21   = float(importes.get('neto_21', 0))
        neto_10_5 = float(importes.get('neto_10_5', 0))
        neto_27   = float(importes.get('neto_27', 0))
        sin_iva   = float(importes.get('importe_total_sin_iva', 0))

        if tipo_norm in TIPOS_SIN_IVA:
            # Sin IVA: todo va como neto
            iva_21 = iva_10_5 = iva_27 = 0.0
            neto_21 = neto_10_5 = neto_27 = 0.0
        else:
            ali_21   = float(alicuotas.get('IVA_21',   {}).get('alicuota', 21.0))
            ali_10_5 = float(alicuotas.get('IVA_10_5', {}).get('alicuota', 10.5))
            ali_27   = float(alicuotas.get('IVA_27',   {}).get('alicuota', 27.0))
            iva_21   = round(neto_21   * ali_21   / 100, 2)
            iva_10_5 = round(neto_10_5 * ali_10_5 / 100, 2)
            iva_27   = round(neto_27   * ali_27   / 100, 2)

        neto_total = neto_21 + neto_10_5 + neto_27 + sin_iva
        iva_total  = iva_21 + iva_10_5 + iva_27

        # Percepciones
        exento    = float(importes.get('importe_exento', 0))
        no_grav   = float(importes.get('importe_no_gravado', 0))
        p_iva     = float(importes.get('perc_iva', 0))
        p_arba    = float(importes.get('perc_arba', 0))
        p_agip    = float(importes.get('perc_agip', 0))
        otros     = float(importes.get('otros_imp', 0))

        # Percepciones CM
        p_cm = sum(float(p.get('importe', 0)) for p in (percepciones_cm or []))

        percepciones_total = p_iva + p_arba + p_agip + p_cm + exento + no_grav + otros
        total = neto_total + iva_total + percepciones_total

        return {
            'neto_total':          round(neto_total, 2),
            'iva_total':           round(iva_total, 2),
            'percepciones_total':  round(percepciones_total, 2),
            'total':               round(total, 2),
            'detalle': {
                'neto_21':   round(neto_21, 2),
                'iva_21':    round(iva_21, 2),
                'neto_10_5': round(neto_10_5, 2),
                'iva_10_5':  round(iva_10_5, 2),
                'neto_27':   round(neto_27, 2),
                'iva_27':    round(iva_27, 2),
                'sin_iva':   round(sin_iva, 2),
                'exento':    round(exento, 2),
                'no_grav':   round(no_grav, 2),
                'perc_iva':  round(p_iva, 2),
                'perc_arba': round(p_arba, 2),
                'perc_agip': round(p_agip, 2),
                'perc_cm':   round(p_cm, 2),
                'otros':     round(otros, 2),
            }
        }

    # ─────────────────────────────────────────────────────────────────────────
    # RESOLUCIÓN DE REGLAS (con herencia enterprise_id=0)
    # ─────────────────────────────────────────────────────────────────────────

    def _resolver_impuestos(self, operacion: str, tipo_norm: str,
                             cond_norm: str, exencion_norm: str = '*', existing_cursor=None) -> list:
        """Resuelve qué impuestos aplican delegando en lógica interna."""
        cache_key = (operacion, tipo_norm, cond_norm, exencion_norm)
        if cache_key in self._cache_reglas:
            return self._cache_reglas[cache_key]

        if existing_cursor:
            result = self._logic_resolver_impuestos(operacion, tipo_norm, cond_norm, exencion_norm, existing_cursor)
        else:
            with get_db_cursor(dictionary=True) as cursor:
                result = self._logic_resolver_impuestos(operacion, tipo_norm, cond_norm, exencion_norm, cursor)
        
        self._cache_reglas[cache_key] = result
        return result

    def _logic_resolver_impuestos(self, operacion, tipo_norm, cond_norm, exencion_norm, cursor):
        cursor.execute("""
            SELECT DISTINCT
                ti.id, ti.codigo, ti.nombre, ti.tipo, ti.orden_display,
                tr.es_obligatorio,
                (CASE WHEN tr.tipo_responsable != '*' THEN 4 ELSE 0 END +
                 CASE WHEN tr.condicion_iibb   != '*' THEN 2 ELSE 0 END +
                 CASE WHEN tr.exencion_iibb    != '*' THEN 1 ELSE 0 END) AS especificidad,
                CASE WHEN tr.enterprise_id = %s THEN 1 ELSE 0 END AS es_propio
            FROM tax_reglas tr
            JOIN tax_impuestos ti ON tr.impuesto_id = ti.id AND (ti.enterprise_id = 0 OR ti.enterprise_id = %s)
            WHERE tr.enterprise_id IN (0, %s)
              AND tr.operacion = %s
              AND tr.aplica = 1
              AND ti.activo = 1
              AND tr.activo = 1
              AND tr.tipo_responsable IN (%s, '*')
              AND tr.condicion_iibb   IN (%s, '*')
              AND tr.exencion_iibb    IN (%s, '*')
            ORDER BY es_propio DESC, especificidad DESC, ti.orden_display ASC
        """, (
            self.enterprise_id, self.enterprise_id, self.enterprise_id,
            operacion, tipo_norm, cond_norm, exencion_norm
        ))
        rows = cursor.fetchall()
        seen = set(); result = []
        for r in rows:
            if r['codigo'] not in seen:
                seen.add(r['codigo']); result.append(r)
        return result

    def _resolver_iibb(self, cond_norm: str, existing_cursor=None) -> list:
        if cond_norm in self._cache_iibb:
            return self._cache_iibb[cond_norm]

        if not cond_norm or cond_norm == 'NINGUNO': return []

        if existing_cursor:
            result = self._logic_resolver_iibb(cond_norm, existing_cursor)
        else:
            with get_db_cursor(dictionary=True) as cursor:
                result = self._logic_resolver_iibb(cond_norm, cursor)

        self._cache_iibb[cond_norm] = result
        return result

    def _logic_resolver_iibb(self, cond_norm, cursor):
        cursor.execute("""
            SELECT
                ri.jurisdiccion_codigo, ri.jurisdiccion_nombre, ri.usa_padron,
                ri.regimen, ri.limite_cm_pct, ri.coef_minimo_cm, ri.alicuota_override,
                ti.codigo as impuesto_codigo,
                CASE WHEN ri.enterprise_id = %s THEN 1 ELSE 0 END as es_propio
            FROM tax_reglas_iibb ri
            JOIN tax_impuestos ti ON ri.impuesto_id = ti.id AND (ti.enterprise_id = 0 OR ti.enterprise_id = %s)
            WHERE ri.enterprise_id IN (0, %s)
              AND ri.condicion_iibb = %s
              AND ri.activo = 1
            ORDER BY es_propio DESC, ri.jurisdiccion_codigo ASC
        """, (self.enterprise_id, self.enterprise_id, self.enterprise_id, cond_norm))
        rows = cursor.fetchall()
        return [
            {
                'codigo': r['jurisdiccion_codigo'], 'nombre': r['jurisdiccion_nombre'],
                'usa_padron': bool(r['usa_padron']), 'regimen': r['regimen'],
                'limite_cm_pct': float(r['limite_cm_pct'] or 100),
                'coef_minimo_cm': float(r['coef_minimo_cm'] or 0),
                'alicuota_override': float(r['alicuota_override']) if r['alicuota_override'] else None,
                'impuesto_codigo': r['impuesto_codigo'],
            }
            for r in rows
        ]

    def _get_alicuotas_vigentes(self, existing_cursor=None) -> dict:
        if self._cache_alicuotas is not None:
            return self._cache_alicuotas

        if existing_cursor:
            result = self._logic_get_alicuotas_vigentes(existing_cursor)
        else:
            with get_db_cursor(dictionary=True) as cursor:
                result = self._logic_get_alicuotas_vigentes(cursor)

        self._cache_alicuotas = result
        return result

    def _logic_get_alicuotas_vigentes(self, cursor):
        hoy = datetime.date.today().isoformat()
        cursor.execute("""
            SELECT ti.codigo, ta.alicuota, ta.base_calculo,
                CASE WHEN ta.enterprise_id = %s THEN 1 ELSE 0 END as es_propio
            FROM tax_alicuotas ta
            JOIN tax_impuestos ti ON ta.impuesto_id = ti.id AND (ti.enterprise_id = 0 OR ti.enterprise_id = %s)
            WHERE ta.enterprise_id IN (0, %s)
              AND ta.activo = 1
              AND ta.vigencia_desde <= %s
              AND (ta.vigencia_hasta IS NULL OR ta.vigencia_hasta >= %s)
            ORDER BY es_propio DESC, ta.vigencia_desde DESC
        """, (self.enterprise_id, self.enterprise_id, self.enterprise_id, hoy, hoy))
        rows = cursor.fetchall()
        result = {}
        for r in rows:
            if r['codigo'] not in result:
                result[r['codigo']] = {'alicuota': float(r['alicuota']), 'base_calculo': r['base_calculo']}
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalizar_tipo(tipo: str) -> str:
        """Normaliza el tipo_responsable para matching con las reglas."""
        return (tipo or '').upper().strip().replace(' ', '_').replace('-', '_')

    @staticmethod
    def _normalizar_iibb(condicion: str) -> str:
        """Normaliza la condición IIBB."""
        return (condicion or '').upper().strip().replace(' ', '_')

    @staticmethod
    def _normalizar_exencion(exencion: str) -> str:
        """
        Normaliza la condición de exención IIBB.
        Valores válidos: 'EXENTO', 'NO_EXENTO', '*' (cualquiera).
        Si viene vacío o desconocido, se asume NO_EXENTO (presunción de que corresponde).
        Esto es clave para CABA: si el padrón NO informa exención, se presume que aplica IIBB.
        """
        val = (exencion or '').upper().strip()
        if val in ('EXENTO', 'EXENTA', 'EX'):
            return 'EXENTO'
        if val in ('*', 'CUALQUIERA', 'TODOS'):
            return '*'
        # Por defecto: NO_EXENTO (si no viene informado, se presume que corresponde IIBB)
        return 'NO_EXENTO'

    @staticmethod
    def _campo_neto(codigo: str) -> str:
        """Mapea código de impuesto → nombre del campo neto en el formulario HTML."""
        mapa = {
            'IVA_21':    'neto_21',
            'IVA_10_5':  'neto_10_5',
            'IVA_27':    'neto_27',
            'PERC_IVA':  'perc_iva',
            'IIBB_ARBA': 'perc_arba',
            'IIBB_AGIP': 'perc_agip',
            'IIBB_CM':   'perc_cm',
            'OTROS_IMP': 'otros_imp',
        }
        return mapa.get(codigo, codigo.lower())

    @staticmethod
    def _campo_impuesto(codigo: str) -> str:
        """Mapea código de impuesto → nombre del campo calculado en el formulario HTML."""
        mapa = {
            'IVA_21':   'iva_21',
            'IVA_10_5': 'iva_10_5',
            'IVA_27':   'iva_27',
        }
        return mapa.get(codigo, '')

    # ─────────────────────────────────────────────────────────────────────────
    # ADMINISTRACIÓN: obtener/actualizar alícuotas de una empresa
    # ─────────────────────────────────────────────────────────────────────────

    def get_config_completa(self) -> dict:
        """
        Retorna la configuración fiscal completa de la empresa para la UI de administración.
        Incluye todas las reglas y alícuotas, indicando si son propias o heredadas.
        """
        with get_db_cursor(dictionary=True) as cursor:
            # Reglas agrupadas por operación
            cursor.execute("""
                SELECT
                    tr.operacion,
                    tr.tipo_responsable,
                    tr.condicion_iibb,
                    ti.codigo as impuesto_codigo,
                    ti.nombre as impuesto_nombre,
                    ti.tipo as impuesto_tipo,
                    tr.aplica,
                    tr.es_obligatorio,
                    CASE WHEN tr.enterprise_id = %s THEN 'PROPIA' ELSE 'HEREDADA' END as origen,
                    tr.enterprise_id
                FROM tax_reglas tr
                JOIN tax_impuestos ti ON tr.impuesto_id = ti.id
                WHERE tr.enterprise_id IN (0, %s)
                  AND tr.activo = 1
                ORDER BY tr.operacion, tr.tipo_responsable, tr.condicion_iibb, ti.orden_display
            """, (self.enterprise_id, self.enterprise_id))
            reglas = cursor.fetchall()

            # Alícuotas
            cursor.execute("""
                SELECT
                    ti.codigo, ti.nombre, ti.tipo,
                    ta.alicuota, ta.base_calculo,
                    ta.vigencia_desde, ta.vigencia_hasta,
                    CASE WHEN ta.enterprise_id = %s THEN 'PROPIA' ELSE 'HEREDADA' END as origen
                FROM tax_alicuotas ta
                JOIN tax_impuestos ti ON ta.impuesto_id = ti.id
                WHERE ta.enterprise_id IN (0, %s)
                  AND ta.activo = 1
                ORDER BY ti.orden_display
            """, (self.enterprise_id, self.enterprise_id))
            alicuotas = cursor.fetchall()

        # Agrupar reglas por operación
        por_operacion = {}
        for r in reglas:
            op = r['operacion']
            if op not in por_operacion:
                por_operacion[op] = []
            por_operacion[op].append(dict(r))

        return {
            'enterprise_id': self.enterprise_id,
            'reglas_por_operacion': por_operacion,
            'alicuotas': [dict(a) for a in alicuotas],
        }

    def actualizar_alicuota(self, codigo_impuesto: str, nueva_alicuota: float,
                             base_calculo: str = 'NETO_GRAVADO',
                             vigencia_desde: str = None) -> bool:
        """
        Crea o actualiza una alícuota específica para esta empresa.
        Si ya existe una alícuota propia vigente, la cierra y crea una nueva.
        """
        hoy = vigencia_desde or datetime.date.today().isoformat()

        with get_db_cursor(dictionary=True) as cursor:
            # Obtener impuesto_id (puede ser global id=0 o propio)
            cursor.execute("SELECT id FROM tax_impuestos WHERE codigo = %s AND (enterprise_id = 0 OR enterprise_id = %s) ORDER BY enterprise_id DESC LIMIT 1", (codigo_impuesto, self.enterprise_id))
            imp = cursor.fetchone()
            if not imp:
                return False

            # Cerrar alícuota vigente anterior (si existe)
            cursor.execute("""
                UPDATE tax_alicuotas
                SET vigencia_hasta = %s, activo = 0
                WHERE enterprise_id = %s AND impuesto_id = %s
                  AND activo = 1
                  AND (vigencia_hasta IS NULL OR vigencia_hasta >= %s)
            """, (hoy, self.enterprise_id, imp['id'], hoy))

            # Insertar nueva alícuota
            cursor.execute("""
                INSERT INTO tax_alicuotas
                    (enterprise_id, impuesto_id, alicuota, base_calculo, vigencia_desde)
                VALUES (%s, %s, %s, %s, %s)
            """, (self.enterprise_id, imp['id'], nueva_alicuota, base_calculo, hoy))

        # Invalidar cache
        self._cache_alicuotas = None
        
        # Opcional: Auto-versionar si se desea
        # self.create_version(f"Actualización alícuota {codigo_impuesto}", user_id=None)
        
        return True

    def create_version(self, descripcion: str, user_id: int = None) -> str:
        """
        Genera una nueva versión completa del motor fiscal.
        Guarda toda la configuración actual (reglas y alicuotas) en un snapshot.
        """
        try:
            with get_db_cursor(dictionary=True) as cursor:
                # 1. Obtener reglas actuales
                cursor.execute("SELECT * FROM tax_reglas WHERE enterprise_id IN (0, %s) AND activo = 1", (self.enterprise_id,))
                reglas = cursor.fetchall()
                
                # 2. Obtener alicuotas actuales
                cursor.execute("SELECT * FROM tax_alicuotas WHERE enterprise_id IN (0, %s) AND activo = 1", (self.enterprise_id,))
                alicuotas = cursor.fetchall()
                
                # 3. Determinar codigo versión (incrementar)
                cursor.execute("""
                    SELECT version_code 
                    FROM tax_engine_versions 
                    WHERE enterprise_id = %s 
                    ORDER BY id DESC LIMIT 1
                """, (self.enterprise_id,))
                last_ver = cursor.fetchone()
                
                new_ver_code = "1.0"
                if last_ver:
                    try:
                        major, minor = last_ver['version_code'].split('.')
                        new_ver_code = f"{major}.{int(minor) + 1}"
                    except:
                        new_ver_code = f"{last_ver['version_code']}.1"
                
                # 4. Insertar Versión
                cursor.execute("""
                    INSERT INTO tax_engine_versions (enterprise_id, version_code, descripcion, usuario_id)
                    VALUES (%s, %s, %s, %s)
                """, (self.enterprise_id, new_ver_code, descripcion, user_id))
                version_id = cursor.lastrowid
                
                # 5. Insertar Snapshot
                # Usamos default=str para serializar fechas/decimales
                reglas_json = json.dumps(reglas, default=str, ensure_ascii=False)
                alicuotas_json = json.dumps(alicuotas, default=str, ensure_ascii=False)
                
                cursor.execute("""
                    INSERT INTO tax_engine_snapshots (enterprise_id, version_id, reglas_json, alicuotas_json)
                    VALUES (%s, %s, %s, %s)
                """, (self.enterprise_id, version_id, reglas_json, alicuotas_json))
                
                return new_ver_code
                
        except Exception as e:
            print(f"Error creating version: {e}")
            return None
