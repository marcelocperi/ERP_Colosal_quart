from database import get_db_cursor
import re

def migrate_and_fix_codes():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            print("--- Iniciando Migración y Corrección de Códigos ---")
            
            # 1. Asegurar columna naturaleza en erp_terceros
            cursor.execute("SHOW COLUMNS FROM erp_terceros LIKE 'naturaleza'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE erp_terceros ADD COLUMN naturaleza VARCHAR(50) DEFAULT NULL AFTER nombre")

            # 2. Migrar desde proveedores si no están en erp_terceros (basado en CUIT)
            print("Migrando proveedores...")
            cursor.execute("SELECT * FROM proveedores")
            for p in cursor.fetchall():
                cursor.execute("SELECT id FROM erp_terceros WHERE cuit = %s AND es_proveedor = 1", (p['cuit'],))
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO erp_terceros (enterprise_id, nombre, cuit, telefono, email, es_proveedor, es_cliente, naturaleza, activo)
                        VALUES (%s, %s, %s, %s, %s, 0, 1, %s, 1)
                    """, (p['enterprise_id'], p['razon_social'], p['cuit'], p['telefono'], p['email'], p['naturaleza']))
                    print(f"   + Migrado proveedor: {p['razon_social']}")

            # 3. Migrar desde clientes si no están en erp_terceros
            print("Migrando clientes...")
            cursor.execute("SELECT * FROM clientes")
            for c in cursor.fetchall():
                cursor.execute("SELECT id FROM erp_terceros WHERE cuit = %s AND es_cliente = 1", (c['cuit'],))
                if not cursor.fetchone():
                    # Para clientes, determinamos naturaleza por tipo_responsable o default CLI
                    nat = "CLI"
                    if c.get('tipo_responsable') == 'RI': nat = "EMP" # Ejemplo: Empresa
                    
                    cursor.execute("""
                        INSERT INTO erp_terceros (enterprise_id, nombre, cuit, telefono, email, es_cliente, es_proveedor, naturaleza, activo, tipo_responsable)
                        VALUES (%s, %s, %s, %s, %s, 1, 0, %s, 1, %s)
                    """, (c['enterprise_id'], c['nombre'], c['cuit'], c['telefono'], c['email'], nat, c['tipo_responsable']))
                    print(f"   + Migrado cliente: {c['nombre']}")

            # 4. Generar Códigos para todos los que no tengan el formato correcto
            print("Generando/Corrigiendo códigos...")
            cursor.execute("SELECT id, nombre, cuit, es_cliente, es_proveedor, naturaleza, codigo FROM erp_terceros")
            terceros = cursor.fetchall()

            counts = {}
            # Inicializar contadores con códigos existentes que sí cumplen el formato
            for t in terceros:
                if t['codigo']:
                    match = re.match(r'^([A-Z]{3})(\d{5})$', t['codigo'])
                    if match:
                        nat_code, num = match.groups()
                        num = int(num)
                        if nat_code not in counts or num > counts[nat_code]:
                            counts[nat_code] = num

            for t in terceros:
                # Si ya tiene un código válido, saltar
                if t['codigo'] and re.match(r'^[A-Z]{3}\d{5}$', t['codigo']):
                    continue
                
                # Naturaleza
                nat_str = t['naturaleza'] or ("CLI" if t['es_cliente'] else "GEN")
                prefix = nat_str[:3].upper()
                if len(prefix) < 3: prefix = (prefix + "XXX")[:3]

                counts[prefix] = counts.get(prefix, 0) + 1
                new_code = f"{prefix}{counts[prefix]:05d}"
                
                cursor.execute("UPDATE erp_terceros SET codigo = %s, naturaleza = %s WHERE id = %s", 
                             (new_code, nat_str, t['id']))
                # print(f"   * {t['nombre']} -> {new_code}")

            print("--- Proceso Completado ---")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    migrate_and_fix_codes()
