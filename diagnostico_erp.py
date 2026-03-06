"""
Diagnóstico completo del sistema ERP después de la migración
"""
from database import get_db_cursor
import sys

def check_tables():
    """Verificar que existan todas las tablas necesarias"""
    print("\n" + "="*60)
    print("1. VERIFICANDO TABLAS DEL SISTEMA")
    print("="*60)
    
    required_tables = [
        'stk_articulos',
        'stk_existencias',
        'stk_movimientos',
        'stk_movimientos_detalle',
        'stk_motivos',
        'stk_depositos',
        'stk_tipos_articulo',
        'erp_terceros',
        'prestamos',
        'usuarios'
    ]
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SHOW TABLES")
            existing_tables = [t[0] for t in cursor.fetchall()]
            
            print(f"\nTablas encontradas: {len(existing_tables)}")
            
            missing = []
            for table in required_tables:
                if table in existing_tables:
                    print(f"  OK {table}")
                else:
                    print(f"  ERR {table} - FALTA")
                    missing.append(table)
            
            if missing:
                print(f"\nAVISO: FALTAN {len(missing)} TABLAS CRITICAS")
                return False
            else:
                print("\nOK: Todas las tablas requeridas existen")
                return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def check_articulos_data():
    """Verificar datos en stk_articulos"""
    print("\n" + "="*60)
    print("2. VERIFICANDO DATOS EN STK_ARTICULOS")
    print("="*60)
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Total de artículos
            cursor.execute("SELECT COUNT(*) as total FROM stk_articulos")
            total = cursor.fetchone()['total']
            print(f"\nTotal de artículos: {total}")
            
            # Por empresa
            cursor.execute("SELECT enterprise_id, COUNT(*) as count FROM stk_articulos GROUP BY enterprise_id")
            for row in cursor.fetchall():
                print(f"  - Empresa {row['enterprise_id']}: {row['count']} artículos")
            
            # Muestra de datos
            cursor.execute("SELECT id, nombre, codigo, modelo, precio_venta FROM stk_articulos LIMIT 5")
            print("\nMuestra de artículos:")
            for art in cursor.fetchall():
                print(f"  ID {art['id']}: {art['nombre']} | ISBN: {art['codigo']} | Autor: {art['modelo']} | Precio: ${art['precio_venta']}")
            
            return total > 0
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def check_stock_routes():
    """Verificar que las rutas de stock estén registradas"""
    print("\n" + "="*60)
    print("3. VERIFICANDO RUTAS DE STOCK")
    print("="*60)
    
    try:
        from app import app
        
        stock_routes = [r for r in app.url_map.iter_rules() if 'stock' in r.rule]
        
        print(f"\nRutas de stock encontradas: {len(stock_routes)}")
        for route in sorted(stock_routes, key=lambda x: x.rule):
            print(f"  {route.methods} {route.rule}")
        
        return len(stock_routes) > 0
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def check_biblioteca_routes():
    """Verificar rutas de biblioteca"""
    print("\n" + "="*60)
    print("4. VERIFICANDO RUTAS DE BIBLIOTECA")
    print("="*60)
    
    try:
        from app import app
        
        bib_routes = [r for r in app.url_map.iter_rules() if 'biblioteca' in r.endpoint or '/prestamos' in r.rule or '/usuarios' in r.rule]
        
        print(f"\nRutas de biblioteca encontradas: {len(bib_routes)}")
        for route in sorted(bib_routes, key=lambda x: x.rule):
            print(f"  {route.methods} {route.rule} -> {route.endpoint}")
        
        return len(bib_routes) > 0
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def check_permissions():
    """Verificar permisos del sistema"""
    print("\n" + "="*60)
    print("5. VERIFICANDO PERMISOS")
    print("="*60)
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT code, description FROM sys_permissions ORDER BY code")
            perms = cursor.fetchall()
            
            print(f"\nPermisos registrados: {len(perms)}")
            for p in perms:
                print(f"  - {p['code']}: {p['description']}")
            
            return len(perms) > 0
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("DIAGNÓSTICO DEL SISTEMA ERP POST-MIGRACIÓN")
    print("="*60)
    
    results = {
        'Tablas': check_tables(),
        'Datos de Artículos': check_articulos_data(),
        'Rutas de Stock': check_stock_routes(),
        'Rutas de Biblioteca': check_biblioteca_routes(),
        'Permisos': check_permissions()
    }
    
    print("\n" + "="*60)
    print("RESUMEN DEL DIAGNÓSTICO")
    print("="*60)
    
    for test, passed in results.items():
        status = "OK" if passed else "FALLA"
        print(f"{status} - {test}")
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\nSISTEMA OPERATIVO - Todos los tests pasaron")
        return 0
    else:
        print("\nSISTEMA CON PROBLEMAS - Revisar fallos arriba")
        return 1

if __name__ == "__main__":
    sys.exit(main())
