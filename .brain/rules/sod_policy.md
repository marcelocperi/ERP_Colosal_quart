# Política de Segregación de Funciones (SoD) - Colosal ERP

## 1. Conflictos Críticos
Un mismo usuario NO puede tener permisos que permitan completar un ciclo transaccional de punto a punto. Ejemplo:
- **Compras**: Quien registra el proveedor no puede autorizar el pago.
- **Ventas**: Quien emite la factura no puede registrar la cobranza en fondos.
- **Stock**: Quien solicita la compra no puede realizar la recepción de mercadería (Remito entrada).

## 2. Niveles de Riesgo
- **ALTO**: Conflictos que involucran salida de dinero o movimiento de inventario real.
- **MEDIO**: Acceso a reportes confidenciales y configuración de maestros.
- **BAJO**: Permisos de visualización de dashboards.

## 3. Protocolo de Violación
Ante un conflicto detectado por el `SodService`, el sistema debe:
1. Registrar la alerta en `sys_security_logs`.
2. Requerir firma del SaaS Owner para excepciones temporales.
3. Bloquear la asignación automática si el riesgo es crítico.
