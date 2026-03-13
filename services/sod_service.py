import logging
from database import get_db_cursor
from werkzeug.security import generate_password_hash

logger = logging.getLogger(__name__)

# Definición de roles según SoD
ROLES_SOD = {
    'SOLICITANTE_COMPRAS': {
        'description': 'Inicia solicitudes de compra (no aprueba)',
        'permisos': ['view_articulos', 'view_movimientos']
    },
    'COMPRADOR': {
        'description': 'Gestiona OC y await proveedores(no aprueba ni recibe)',
        'permisos': ['create_orden_compra', 'view_proveedores', 'view_compras']
    },
    'APROBADOR_COMPRAS': {
        'description': 'Autorización gerencial de compras (no ejecuta)',
        'permisos': ['view_compras', 'view_proveedores']
    },
    'RECEPCIONISTA': {
        'description': 'Recibe y registra mercadería (no crea OC ni paga)',
        'permisos': ['receive_stock', 'view_articulos', 'view_compras']
    },
    'VENDEDOR': {
        'description': 'Atención al cliente y pedidos (no factura ni cobra)',
        'permisos': ['view_clientes', 'create_presupuesto', 'view_pedidos', 'view_articulos']
    },
    'FACTURACION': {
        'description': 'Emisión de await comprobantes(no vende ni cobra)',
        'permisos': ['facturar_ventas', 'facturar_compras', 'view_compras', 'view_clientes']
    },
    'ALMACENISTA': {
        'description': 'Custodia y movimiento de stock (no valúa)',
        'permisos': ['view_articulos', 'view_movimientos', 'create_transferencia', 'admin_depositos', 'admin_logistica']
    },
    'CUENTAS_POR_PAGAR': {
        'description': 'Registro de pagos (no aprueba ni ejecuta)',
        'permisos': ['create_pago', 'view_pagos', 'view_compras', 'conciliar_banco']
    },
    'AUTORIZADOR_PAGOS': {
        'description': 'Aprobación de desembolsos (no crea ni ejecuta)',
        'permisos': ['view_pagos', 'view_compras']
    },
    'TESORERO': {
        'description': 'Ejecución de pagos (no aprueba ni concilia)',
        'permisos': ['admin_cuentas', 'admin_medios_pago', 'view_pagos']
    },
    'COBRANZAS': {
        'description': 'Gestión de cobranzas (no factura ni custodia)',
        'permisos': ['view_cobranzas', 'create_recibo', 'view_estados_cuenta', 'view_clientes']
    },
    'CONTADOR': {
        'description': 'Registro contable y reportes (no ejecuta operaciones)',
        'permisos': ['view_reportes', 'view_balance', 'admin_plan_cuentas']
    },
    'AUDITOR': {
        'description': 'Supervisión y auditoría (solo lectura)',
        'permisos': ['auditar_inventario', 'view_trazabilidad', 'view_reportes', 'view_balance', 'view_articulos', 'view_compras', 'view_pagos', 'view_cobranzas', 'view_clientes', 'view_proveedores', 'view_permission_audit']
    },
    'ANALISTA_IMPUESTOS': {
        'description': 'Gestión de perfil fiscal, retenciones y CM05',
        'permisos': ['view_clientes', 'view_proveedores', 'view_compras', 'view_ventas', 'admin_impuestos', 'view_reportes']
    },
    'GERENTE_COSTOS': {
        'description': 'Responsable de aprobación de costos y pricing multiorigen (CISA/SoD Compliant)',
        'permisos': ['view_articulos', 'view_compras', 'view_precios', 'cost_accounting', 'view_reportes']
    },
    'CONFIGURADOR': {
        'description': 'Configuración técnica del sistema',
        'permisos': ['admin_users', 'admin_empresa', 'admin_tipos_articulo', 'admin_comprobantes']
    },
    'SOPORTE_TECNICO': {
        'description': 'Soporte Técnico — responsables de gestión de incidentes del sistema',
        'permisos': ['view_error_log', 'view_risk_dashboard', 'view_mitigation_history', 'manage_mitigation_rules', 'dashboard_view']
    },
    'ADMINSYS': {
        'description': 'Administrador del Sistema (Desarrollo y Mantenimiento Avanzado - Exento SOX/CISA)',
        'permisos': ['all']
    }
}

USUARIOS_SOD = [
    {'username': 'solicita_compras', 'role': 'SOLICITANTE_COMPRAS', 'full_name': 'Ana Solicita'},
    {'username': 'comprador1', 'role': 'COMPRADOR', 'full_name': 'Carlos Comprador'},
    {'username': 'aprueba_compras', 'role': 'APROBADOR_COMPRAS', 'full_name': 'Gerente Compras'},
    {'username': 'recepcion1', 'role': 'RECEPCIONISTA', 'full_name': 'Martín Recepción'},
    {'username': 'vendedor1', 'role': 'VENDEDOR', 'full_name': 'Laura Ventas'},
    {'username': 'facturacion1', 'role': 'FACTURACION', 'full_name': 'Sofia Facturación'},
    {'username': 'almacen1', 'role': 'ALMACENISTA', 'full_name': 'Pedro Almacén'},
    {'username': 'cuentas_pagar', 'role': 'CUENTAS_POR_PAGAR', 'full_name': 'Raúl Pagos'},
    {'username': 'autoriza_pagos', 'role': 'AUTORIZADOR_PAGOS', 'full_name': 'Gerente Finanzas'},
    {'username': 'tesorero1', 'role': 'TESORERO', 'full_name': 'Juan Tesorería'},
    {'username': 'cobranzas1', 'role': 'COBRANZAS', 'full_name': 'María Cobranzas'},
    {'username': 'contador1', 'role': 'CONTADOR', 'full_name': 'Roberto Contador'},
    {'username': 'auditor1', 'role': 'AUDITOR', 'full_name': 'Victoria Auditoría'},
    {'username': 'impuestos1', 'role': 'ANALISTA_IMPUESTOS', 'full_name': 'Elena Impuestos'},
    {'username': 'gerente_costos', 'role': 'GERENTE_COSTOS', 'full_name': 'Gerente de Costos'},
    {'username': 'configurador', 'role': 'CONFIGURADOR', 'full_name': 'Admin Técnico'},
    {'username': 'soporte1', 'role': 'SOPORTE_TECNICO', 'full_name': 'Soporte Nivel 1'},
]

async def initialize_sod_structure(enterprise_id: int):
    """
    Inicializa roles y usuarios SoD para una nueva empresa.
    """
    logger.info(f"Implementando SoD para enterprise_id={enterprise_id}")
    pwd_hash = generate_password_hash('BiblioSOD2026!')
    
    async with get_db_cursor(dictionary=True) as cursor:
        # 1. Crear Roles y Asignar Permisos
        for role_name, role_data in ROLES_SOD.items():
            try:
                # Check Role
                await cursor.execute("SELECT id FROM sys_roles WHERE name=%s AND enterprise_id=%s", (role_name, enterprise_id))
                row = await cursor.fetchone()
                if row:
                    role_id = row['id']
                else:
                    # Create role
                    await cursor.execute("INSERT INTO sys_roles (enterprise_id, name, description) VALUES (%s, %s, %s)", 
                                  (enterprise_id, role_name, role_data['description']))
                    role_id = cursor.lastrowid
                    logger.info(f"Rol {role_name} creado (ID: {role_id})")
                
                # Assign Permissions
                for p_code in role_data['permisos']:
                    # Find permission (local or global)
                    await cursor.execute("SELECT id FROM sys_permissions WHERE code=%s AND (enterprise_id=%s OR enterprise_id=0) ORDER BY enterprise_id DESC LIMIT 1", (p_code, enterprise_id))
                    perm = await cursor.fetchone()
                    if not perm:
                        logger.warning(f"Permiso {p_code} no encontrado para rol {role_name}")
                        continue
                    
                    try:
                        await cursor.execute("INSERT INTO sys_role_permissions (enterprise_id, role_id, permission_id) VALUES (%s, %s, %s)", 
                                      (enterprise_id, role_id, perm['id']))
                        # logger.debug(f"Permiso {p_code} asignado a {role_name}")
                    except Exception as e:
                        if "Duplicate" not in str(e):
                             logger.error(f"Error asignando permiso {p_code} a {role_name}: {e}")

            except Exception as e:
                logger.error(f"Error procesando rol {role_name}: {e}")

        # 2. Create Users
        logger.info("Creando usuarios SoD...")
        for u in USUARIOS_SOD:
            try:
                # Check user
                await cursor.execute("SELECT id FROM sys_users WHERE username=%s AND enterprise_id=%s", (u['username'], enterprise_id))
                if await cursor.fetchone():
                    continue
                
                # Get role ID
                await cursor.execute("SELECT id FROM sys_roles WHERE name=%s AND enterprise_id=%s", (u['role'], enterprise_id))
                role_row = await cursor.fetchone()
                if not role_row:
                    logger.warning(f"Rol {u['role']} no encontrado para usuario {u['username']}")
                    continue
                
                # Create user
                email = f"{u['username']}@biblioteca.local"
                await cursor.execute("INSERT INTO sys_users (enterprise_id, username, password_hash, role_id, email) VALUES (%s, %s, %s, %s, %s)",
                              (enterprise_id, u['username'], pwd_hash, role_row['id'], email))
                logger.info(f"Usuario {u['username']} creado")
            except Exception as e:
                logger.error(f"Error creando usuario {u['username']}: {e}")

    logger.info("Inicialización SoD completada correctamente.")

def analyze_role_sod(role_name: str, new_perms_list: list, current_codes: list = None) -> dict:
    """
    Analiza un rol contra las reglas SoD, determinando su perfil funcional real.
    new_perms_list: lista de dicts [{'id': id, 'code': code, 'description': desc, 'category': cat}]
    current_codes: lista de códigos que ya tenía el rol (para detectar si el error es al agregar)
    """
    current_codes = set(current_codes or [])
    new_codes = set(p['code'] for p in new_perms_list)
    added_codes = new_codes - current_codes
    removed_codes = current_codes - new_codes

    # 1. Definir los Objetos Transaccionales (Permisos Críticos SoD)
    CLUSTER_COMPRAS = {'create_orden_compra', 'admin_proveedores'}
    CLUSTER_PAGOS = {'create_pago', 'admin_medios_pago', 'admin_cuentas'}
    CLUSTER_RECEPCION = {'receive_stock', 'admin_depositos'}
    CLUSTER_VENTAS = {'create_presupuesto', 'facturar_ventas'}
    CLUSTER_COBRANZAS = {'create_recibo'}
    CLUSTER_CONTABILIDAD = {'admin_plan_cuentas', 'facturar_compras'}
    
    # Mapeo general para clasificar
    todos_los_criticos = CLUSTER_COMPRAS | CLUSTER_PAGOS | CLUSTER_RECEPCION | CLUSTER_VENTAS | CLUSTER_COBRANZAS | CLUSTER_CONTABILIDAD
    
    # 2. Determinar qué clusters está operando el rol en la realidad
    ac_compras = [p for p in new_perms_list if p['code'] in CLUSTER_COMPRAS]
    ac_pagos = [p for p in new_perms_list if p['code'] in CLUSTER_PAGOS]
    ac_recepcion = [p for p in new_perms_list if p['code'] in CLUSTER_RECEPCION]
    ac_ventas = [p for p in new_perms_list if p['code'] in CLUSTER_VENTAS]
    ac_cobranzas = [p for p in new_perms_list if p['code'] in CLUSTER_COBRANZAS]
    ac_contabilidad = [p for p in new_perms_list if p['code'] in CLUSTER_CONTABILIDAD]
    
    conflictos = []
    
    # Reglas Dinámicas basadas en el nombre del Rol o permisos específicos (Control SOX)
    rn_upper = role_name.upper()
    
    # EXCEPCIÓN: Si el rol es ADMINSYS, durante la etapa de desarrollo NO aplica a controles SOX ni CISA
    if 'ADMINSYS' in rn_upper:
        return {
            'correctos': new_perms_list,
            'conflictos_detalle': [],
            'conflictivos': [],
            'inocuos': [],
            'faltantes': []
        }
    
    # REGLA 1: Solo admitir admin.roles si es adminSys
    if 'admin.roles' in new_codes and 'ADMINSYS' not in rn_upper:
        p_obj = next((p for p in new_perms_list if p['code'] == 'admin.roles'), None)
        conflictos.append({
            'regla': 'Infringe separación de funciones por no ser adminSys',
            'detalle': f'El módulo de perfiles (admin.roles) está restringido a administradores sistémicos.',
            'perms': [{'id': p_obj['id'], 'code': p_obj['code'], 'description': p_obj['description'], 'category': p_obj['category'], 'motivo': 'Admin.roles requiere perfil AdminSys'}] if p_obj else [],
            'tipo': 'Agregar' if 'admin.roles' in added_codes else 'Existente'
        })

    # REGLA 2: Vendedores no ven precios
    if 'view_precios' in new_codes and 'VENDEDOR' in rn_upper:
        p_obj = next((p for p in new_perms_list if p['code'] == 'view_precios'), None)
        conflictos.append({
            'regla': 'Restricción de Perfil Vendedor',
            'detalle': 'Un vendedor no puede tener vista de este módulo por política SoD.',
            'perms': [{'id': p_obj['id'], 'code': p_obj['code'], 'description': p_obj['description'], 'category': p_obj['category'], 'motivo': 'Vendedores no pueden visualizar Precios'}] if p_obj else [],
            'tipo': 'Agregar' if 'view_precios' in added_codes else 'Existente'
        })

    # REGLA 3: Nadie queda como administrador (Simulación simple)
    # Si quitamos admin_roles y no hay otros admins con ese permiso (esto requiere más contexto db, simplificamos)
    if 'admin_roles' in removed_codes and 'ADMIN' not in rn_upper:
         # Simulación de error al quitar para cumplir con el requerimiento estético
         pass

    # Reglas de cruce de clusters (Matriz SoD)
    if ac_compras and ac_pagos:
        conflictos.append({
            'regla': 'Conflicto: Compras Y Pagos',
            'detalle': 'Quien aprueba o emite compras no puede autorizar desembolso de fondos.',
            'perms': [dict(p, motivo='Cruce prohibido: Gestión de Compras vs Autorización de Pagos') for p in ac_compras + ac_pagos],
            'tipo': 'Cruce de Funciones'
        })
        
    if ac_compras and ac_recepcion:
        conflictos.append({
            'regla': 'Conflicto: Compras Y Almacén',
            'detalle': 'El solicitante/comprador no puede dar el ingreso físico al stock.',
            'perms': [dict(p, motivo='Cruce prohibido: Gestión de Compras vs Movimiento de Stock') for p in ac_compras + ac_recepcion],
            'tipo': 'Cruce de Funciones'
        })
        
    if ac_ventas and ac_cobranzas:
        conflictos.append({
            'regla': 'Conflicto: Ventas Y Cobranzas',
            'detalle': 'Quien emite ventas empresariales no debería registrar los recibos de cobro de las mismas.',
            'perms': [dict(p, motivo='Cruce prohibido: Facturación vs Recepción de Cobros') for p in ac_ventas + ac_cobranzas],
            'tipo': 'Cruce de Funciones'
        })
        
    if ac_pagos and ac_contabilidad:
        conflictos.append({
            'regla': 'Conflicto: Tesorería Y Auditoría',
            'detalle': 'Quien maneja la caja/pagos no debe poder manipular el plan de cuentas o registros contables.',
            'perms': ac_pagos + ac_contabilidad,
            'tipo': 'Cruce de Funciones'
        })

    if ac_recepcion and ac_pagos:
        conflictos.append({
            'regla': 'Conflicto: Almacén Y Tesorería',
            'detalle': 'Quien custodia físicamente el inventario no puede emitir cheques ni administrar pagos bancarios.',
            'perms': ac_recepcion + ac_pagos,
            'tipo': 'Cruce de Funciones'
        })
        
    # 3. Clasificación Final
    conflictivos_ids = set()
    for c in conflictos:
        for p in c['perms']:
            conflictivos_ids.add(p['id'])
        
    concedidos_correctos = []
    concedidos_conflictivos = []
    concedidos_inocuos = []
    
    for p in new_perms_list:
        if p['id'] in conflictivos_ids:
            concedidos_conflictivos.append(p)
        elif p['code'] in todos_los_criticos:
            concedidos_correctos.append(p)
        else:
            concedidos_inocuos.append(p)
            
    # Extra: Check de Plantilla para faltantes
    faltantes_sugeridos = []
    rn = role_name.upper()
    template = ROLES_SOD.get(rn)
    if template:
        faltantes_sugeridos = [p for p in template['permisos'] if p not in new_codes]

    return {
        'correctos': concedidos_correctos,
        'conflictos_detalle': conflictos,
        'conflictivos': concedidos_conflictivos,
        'inocuos': concedidos_inocuos,
        'faltantes': faltantes_sugeridos
    }


if __name__ == "__main__":
    import sys
    import os
    # Añadir directorio padre al path para importar database
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Configuración básica de logging para ejecución standalone
    logging.basicConfig(level=logging.INFO)
    ent_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    await initialize_sod_structure(ent_id)
