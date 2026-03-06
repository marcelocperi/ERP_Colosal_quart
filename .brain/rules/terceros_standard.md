# Estándar de Gestión de Terceros (Clientes/Proveedores) - Colosal ERP

## 1. Identificación Única
- Todo tercero debe ser validado por su CUIT/CUIL (Módulo 11).
- No se permiten registros duplicados por identificación fiscal.

## 2. Categorización Fiscal
- Es obligatorio definir la Condición frente al IVA (RI, Monotributo, Exento).
- Los proveedores deben tener asociada una cuenta contable de pasivo por defecto.

## 3. Seguridad de Datos
- Las cuentas bancarias (CBU/Alias) de proveedores solo pueden ser modificadas por usuarios con nivel 'FINANCE_ADMIN'.
- Cualquier cambio en datos sensibles de terceros debe generar un log en `sys_security_logs` con el valor anterior y el nuevo.
