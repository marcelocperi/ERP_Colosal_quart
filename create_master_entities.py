# -*- coding: utf-8 -*-
"""
Script Maestro de Entidades 4.0 (Full ALTER)
===========================================
Asegura TODAS las columnas necesarias en clientes y proveedores.
Genera datos maestros si faltan.
"""
import sys, os, io, random

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Path setup
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'multiMCP'))
from database import get_db_cursor

# --- DATOS ---
NOMBRES = ['Juan', 'Maria', 'Carlos', 'Ana', 'Luis', 'Sofia', 'Pedro', 'Lucia', 'Miguel', 'Elena',
           'Jorge', 'Valentina', 'Diego', 'Camila', 'Sergio', 'Paula', 'Andres', 'Martina']
APELLIDOS = ['Garcia', 'Rodriguez', 'Gonzalez', 'Fernandez', 'Lopez', 'Martinez', 'Sanchez', 'Perez',
             'Gomez', 'Martin', 'Jimenez', 'Ruiz', 'Hernandez', 'Diaz', 'Moreno', 'Alvarez']
EMPRESAS_SUFIJOS = ['S.A.', 'S.R.L.', 'S.A.S.']
CALLES = ['Av. Rivadavia', 'Av. Santa Fe', 'Calle Corrientes', 'Calle Florida', 'Av. 9 de Julio']
LOCALIDADES = ['CABA', 'Vicente Lopez', 'San Isidro', 'Martinez']

RUBROS = {
    'EDITORIAL': ['Ed. Planeta', 'Penguin RH', 'Siglo XXI', 'Ed. Colihue', 'Kapelusz'],
    'TECNOLOGIA': ['Compumundo', 'Fravega', 'TecnoHard', 'PcSoftware', 'SiliconArg'],
    'INSUMOS': ['Libreria Comercial', 'Office Depot', 'Papelera del Plata', 'Insumos Graficos'],
    'LIMPIEZA': ['Limpieza Total', 'Servicios Aseo', 'Prod. Brillantes', 'CleanMaster'],
    'MANTENIMIENTO': ['Mantenimiento Edilicio', 'ElectroService', 'Plomeria Express'],
    'SERVICIOS': ['Edenor', 'Edesur', 'Metrogas', 'AySA', 'Telecom']
}

def generar_cuit_valido(tipo):
    """Genera CUIT Valido modulo 11."""
    while True:
        if tipo == 'PERSONA': prefijo = random.choice(['20', '23', '27'])
        else: prefijo = random.choice(['30', '33'])
        
        dni = str(random.randint(10000000, 99999999))
        base = prefijo + dni
        
        factores = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2]
        suma = sum(int(d) * f for d, f in zip(base, factores))
        resto = suma % 11
        
        if resto == 0: dv = 0
        elif resto == 1: 
            if tipo == 'PERSONA' and prefijo == '20': base = '23' + dni; pass 
            elif resto == 1: continue 
            continue
        else: dv = 11 - resto
        return f"{base}{dv}"

def asegurar_cols(c, tabla, columna, definicion):
    try:
        c.execute(f"SHOW COLUMNS FROM {tabla} LIKE '{columna}'")
        if not c.fetchone():
            print(f"   + Agregando columna '{columna}' a {tabla}...")
            c.execute(f"ALTER TABLE {tabla} ADD COLUMN {columna} {definicion}")
            return True
    except Exception as e:
        print(f"Error verificando columna {columna} en {tabla}: {e}")
    return False

def asegurar_estructura(c):
    print("--- Verificando Estructura de Tablas ---")
    
    # --- CLIENTES ---
    c.execute("CREATE TABLE IF NOT EXISTS clientes (id INT PRIMARY KEY AUTO_INCREMENT)")
    asegurar_cols(c, 'clientes', 'enterprise_id', "INT DEFAULT 0")
    asegurar_cols(c, 'clientes', 'codigo', "VARCHAR(20) UNIQUE")
    asegurar_cols(c, 'clientes', 'nombre', "VARCHAR(100) NOT NULL")
    asegurar_cols(c, 'clientes', 'cuit', "VARCHAR(13) UNIQUE")
    asegurar_cols(c, 'clientes', 'direccion', "VARCHAR(255)")
    asegurar_cols(c, 'clientes', 'localidad', "VARCHAR(100)")
    asegurar_cols(c, 'clientes', 'telefono', "VARCHAR(50)")
    asegurar_cols(c, 'clientes', 'email', "VARCHAR(100)")
    asegurar_cols(c, 'clientes', 'tipo_responsable', "ENUM('RI', 'MONOTRIBUTO', 'EXENTO', 'CONSUMIDOR_FINAL') DEFAULT 'CONSUMIDOR_FINAL'")
    asegurar_cols(c, 'clientes', 'activo', "BOOLEAN DEFAULT TRUE")
    asegurar_cols(c, 'clientes', 'created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    
    try: c.execute("ALTER TABLE clientes ADD INDEX idx_ent (enterprise_id)")
    except: pass

    # --- PROVEEDORES ---
    c.execute("CREATE TABLE IF NOT EXISTS proveedores (id INT PRIMARY KEY AUTO_INCREMENT)")
    asegurar_cols(c, 'proveedores', 'enterprise_id', "INT DEFAULT 0")
    asegurar_cols(c, 'proveedores', 'codigo', "VARCHAR(20) UNIQUE")
    asegurar_cols(c, 'proveedores', 'razon_social', "VARCHAR(100) NOT NULL")
    asegurar_cols(c, 'proveedores', 'naturaleza', "VARCHAR(50) DEFAULT 'GENERAL'")
    asegurar_cols(c, 'proveedores', 'cuit', "VARCHAR(13) UNIQUE")
    asegurar_cols(c, 'proveedores', 'direccion', "VARCHAR(255)")
    asegurar_cols(c, 'proveedores', 'localidad', "VARCHAR(100)")
    asegurar_cols(c, 'proveedores', 'telefono', "VARCHAR(50)")
    asegurar_cols(c, 'proveedores', 'email', "VARCHAR(100)")
    asegurar_cols(c, 'proveedores', 'condicion_iva', "ENUM('RI', 'MONOTRIBUTO', 'EXENTO') DEFAULT 'RI'")
    asegurar_cols(c, 'proveedores', 'activo', "BOOLEAN DEFAULT TRUE")
    asegurar_cols(c, 'proveedores', 'created_at', "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    try: c.execute("ALTER TABLE proveedores ADD INDEX idx_ent (enterprise_id)")
    except: pass
    try: c.execute("ALTER TABLE proveedores ADD INDEX idx_naturaleza (naturaleza)")
    except: pass

def poblar_proveedores(c):
    print("\n--- Generando Proveedores ---")
    
    try:
        c.execute("SELECT count(*) as cnt FROM proveedores WHERE enterprise_id=0")
        cnt = c.fetchone()['cnt']
    except: cnt = 0

    if cnt > 10: 
        print(f"   Ya existen {cnt} proveedores. Saltando.")
        return

    # Cargar datos existentes
    codigos_usados = set()
    try:
        c.execute("SELECT codigo FROM proveedores WHERE codigo IS NOT NULL")
        for r in c.fetchall(): codigos_usados.add(r['codigo'])
    except: pass
    
    cuits_usados = set()
    try:
        c.execute("SELECT cuit FROM proveedores WHERE cuit IS NOT NULL")
        for r in c.fetchall(): cuits_usados.add(r['cuit'])
    except: pass

    total = 0
    for rubro, lista in RUBROS.items():
        prefijo = rubro[:3]
        pendientes = max(3, len(lista))
        
        for i in range(pendientes):
            if i < len(lista): nombre = lista[i]
            else: nombre = f"{lista[0]} Sucursal {i+1}"
            
            # Check nombre
            try:
                c.execute("SELECT count(*) as cnt FROM proveedores WHERE razon_social = %s", (nombre,))
                if c.fetchone()['cnt'] > 0: continue
            except: pass

            # Generate unique code
            num = 1
            while True:
                codigo = f"{prefijo}-{num:04d}"
                if codigo not in codigos_usados:
                    codigos_usados.add(codigo)
                    break
                num += 1
            
            # Generate unique CUIT
            for _ in range(50):
                cuit = generar_cuit_valido('EMPRESA')
                if cuit not in cuits_usados:
                    cuits_usados.add(cuit)
                    break
            
            try:
                c.execute("""
                    INSERT INTO proveedores 
                    (enterprise_id, codigo, razon_social, naturaleza, cuit, direccion, localidad, condicion_iva, activo)
                    VALUES (0, %s, %s, %s, %s, %s, %s, 'RI', 1)
                """, (codigo, nombre, rubro, cuit, f"Calle {random.randint(1,999)}", "CABA"))
                print(f"   + {codigo}: {nombre}")
                total += 1
            except Exception as e:
                print(f"   Err {nombre}: {e}")

    print(f"   Proveedores generados: {total}")

def poblar_clientes(c):
    print("\n--- Generando Clientes ---")
    try:
        c.execute("SELECT count(*) as cnt FROM clientes WHERE enterprise_id=0")
        actuales = c.fetchone()['cnt']
    except: actuales = 0
        
    objetivo = 50
    if actuales >= objetivo:
        print(f"   Ya existen {actuales} clientes. Saltando.")
        return

    codigos_usados = set()
    try:
        c.execute("SELECT codigo FROM clientes WHERE codigo IS NOT NULL")
        for r in c.fetchall(): codigos_usados.add(r['codigo'])
    except: pass
    
    cuits_usados = set()
    try:
        c.execute("SELECT cuit FROM clientes WHERE cuit IS NOT NULL")
        for r in c.fetchall(): cuits_usados.add(r['cuit'])
    except: pass

    generados = 0
    intentos = 0
    max_intentos = (objetivo - actuales) * 5
    
    while generados < (objetivo - actuales) and intentos < max_intentos:
        intentos += 1
        es_empresa = random.random() < 0.2
        
        if es_empresa:
            nombre = f"{random.choice(APELLIDOS)} {random.choice(EMPRESAS_SUFIJOS)}"
            tipo = 'EMPRESA'; iva = 'RI'; prefijo = 'EMP'
        else:
            nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
            tipo = 'PERSONA'; iva = 'CONSUMIDOR_FINAL'; prefijo = 'CLI'
            
        cuit = generar_cuit_valido(tipo)
        if cuit in cuits_usados: continue
        
        num = 1
        codigo = ""
        while True:
            codigo = f"{prefijo}-{num:04d}"
            if codigo not in codigos_usados:
                codigos_usados.add(codigo)
                break
            num += 1
            
        try:
            c.execute("""
                INSERT INTO clientes 
                (enterprise_id, codigo, nombre, cuit, direccion, localidad, tipo_responsable, activo)
                VALUES (0, %s, %s, %s, %s, %s, %s, 1)
            """, (codigo, nombre, cuit, f"{random.choice(CALLES)} {random.randint(100,5000)}", 
                  random.choice(LOCALIDADES), iva))
            generados += 1
            cuits_usados.add(cuit)
            if generados % 10 == 0: print(f"   .. {generados} clientes")
        except Exception as e:
            print(f"Err cli: {e}")
            pass
            
    print(f"   Total Clientes generados: {generados}")

def main():
    try:
        with get_db_cursor(dictionary=True) as c:
            asegurar_estructura(c)
            poblar_proveedores(c)
            poblar_clientes(c)
        print("\n=== FIN PROCESO ===")
    except Exception as e:
        print(f"CRASH: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
