# Sistema de Inicialización de Empresas - Documentación

## Resumen

Este sistema gestiona la inicialización automática de datos maestros cuando se crea una nueva empresa en el sistema multi-tenant.

## Componentes

### 1. Archivo de Configuración
**Ubicación**: `.agent/enterprise_init_config.json`

Define qué tablas contienen datos maestros que deben copiarse a nuevas empresas.

**Categorías de tablas**:
- **Contabilidad**: Plan de cuentas, tipos de asientos
- **Stock**: Tipos de artículos, motivos de ajuste
- **ERP General**: Puestos, contactos, cuentas de fondos
- **Sistema**: Roles, permisos

### 2. Módulo de Inicialización
**Ubicación**: `services/enterprise_init.py`

Proporciona funciones para:
- `initialize_enterprise_master_data(enterprise_id)`: Copia datos maestros a una nueva empresa
- `get_master_data_summary(enterprise_id)`: Obtiene resumen de datos maestros de una empresa

### 3. Datos Globales (Enterprise ID = 0)

Todos los datos maestros se almacenan en `enterprise_id = 0` (empresa global).
Cuando se crea una nueva empresa, estos datos se copian automáticamente.

## Tablas Maestras Migradas

### ✅ Contabilidad
- **cont_plan_cuentas** (36 registros): Plan de cuentas contable estándar

### ✅ Stock
- **stk_tipos_articulo** (5 registros): Tipos de artículos (Libros, Repuestos, etc.)
- **stk_tipos_articulo_servicios** (2 registros): Servicios externos asociados
- **stk_motivos** (18 registros): Motivos de ajuste de stock
- **stock_motivos** (13 registros): Motivos de movimientos (legacy)

### ✅ ERP General
- **erp_puestos** (15 registros): Puestos/Funciones organizacionales
- **erp_contactos** (4 registros): Contactos genéricos
- **erp_cuentas_fondos** (2 registros): Tipos de cuentas de fondos

### ✅ Sistema
- **sys_roles** (5 registros): Roles de usuario estándar
- **sys_permissions** (19 registros): Permisos del sistema
- **sys_role_permissions** (71 registros): Asignación de permisos a roles

## Tablas Transaccionales (NO se copian)

Estas tablas contienen datos específicos de cada empresa y NO deben copiarse:

- `cotizacion_dolar`: Cotizaciones históricas
- `erp_comprobantes`: Comprobantes emitidos
- `erp_comprobantes_detalle`: Detalles de comprobantes
- `erp_datos_fiscales`: Datos fiscales de terceros
- `historial_prestamos`: Historial de préstamos
- `legacy_libros`: Libros importados
- `movimientos_pendientes`: Movimientos pendientes
- `prestamos`: Préstamos activos
- `stock_ajustes`: Ajustes de stock
- `stk_articulos`: Artículos en stock
- `sys_security_logs`: Logs de seguridad
- `sys_users`: Usuarios del sistema

## Uso

### Al crear una nueva empresa

```python
from services.enterprise_init import initialize_enterprise_master_data

# Crear la empresa en sys_enterprises
new_enterprise_id = create_enterprise(...)

# Inicializar datos maestros
results = initialize_enterprise_master_data(new_enterprise_id)

print(f"Total de registros copiados: {results['total_records']}")
for table, info in results['tables_copied'].items():
    if info['status'] == 'success':
        print(f"  {table}: {info['records_copied']} registros")
```

### Ver resumen de datos maestros

```python
from services.enterprise_init import get_master_data_summary

summary = get_master_data_summary(enterprise_id)

for category, tables in summary.items():
    print(f"\n{category}:")
    for table, info in tables.items():
        print(f"  {table}: {info['count']} registros")
```

## Orden de Inicialización

Las tablas se copian en este orden para respetar dependencias:

1. `sys_roles`
2. `sys_permissions`
3. `sys_role_permissions`
4. `cont_plan_cuentas`
5. `stk_tipos_articulo`
6. `stk_tipos_articulo_servicios`
7. `stk_motivos`
8. `stock_motivos`
9. `erp_cuentas_fondos`
10. `erp_puestos`
11. `erp_contactos`

## Mantenimiento

### Agregar una nueva tabla maestra

1. Editar `.agent/enterprise_init_config.json`
2. Agregar la tabla en la categoría apropiada
3. Agregar el nombre de la tabla en `initialization_order`
4. Asegurarse de que los datos existan en `enterprise_id = 0`

### Migrar datos existentes a global

```python
# Usar el script de migración
python migrate_master_data.py
```

## Notas Importantes

- ⚠️ **NUNCA** copiar tablas transaccionales
- ⚠️ Los datos en `enterprise_id = 0` son la fuente de verdad
- ⚠️ Mantener el orden de inicialización para respetar dependencias
- ✅ Siempre verificar que no existan datos antes de copiar
- ✅ Registrar todas las operaciones en logs

## Historial de Cambios

**2026-02-14**: 
- Migración inicial de datos maestros de empresa 1 a empresa 0
- Creación del sistema de inicialización automática
- Migración de 58 registros en 4 categorías de tablas
