from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES")
    tables = [list(t.values())[0] for t in cursor.fetchall()]
    
    related = [t for t in tables if any(kw in t.lower() for kw in ['devolucion', 'retorno', 'logistica', 'solicitud', 'recibo'])]
    print("Related Tables Found:", related)
    
    for t in related:
        print(f"\nSchema of {t}:")
        cursor.execute(f"DESCRIBE {t}")
        for col in cursor.fetchall():
            print(f"  {col['Field']} - {col['Type']}")

    # Also check erp_comprobantes schema for clues
    print("\nSchema of erp_comprobantes:")
    cursor.execute("DESCRIBE erp_comprobantes")
    for col in cursor.fetchall():
        print(f"  {col['Field']} - {col['Type']}")
