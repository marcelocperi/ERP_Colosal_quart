from database import get_db_cursor

def test_search(enterprise_id, naturaleza='', query=''):
    with get_db_cursor(dictionary=True) as cursor:
        sql = """
            SELECT a.id, a.nombre, a.precio_venta as precio, t.naturaleza, a.codigo 
            FROM stk_articulos a
            LEFT JOIN stk_tipos_articulo t ON a.tipo_articulo_id = t.id
            WHERE a.enterprise_id = %s AND a.activo = 1
        """
        params = [enterprise_id]
        
        if naturaleza:
            sql += " AND t.naturaleza = %s"
            params.append(naturaleza)
            
        if query:
            sql += " AND (a.nombre LIKE %s OR a.codigo LIKE %s OR t.nombre LIKE %s)"
            search = f"%{query}%"
            params.extend([search, search, search])
            
        sql += " ORDER BY a.nombre LIMIT 100"
        
        print(f"Executing: {sql} with {params}")
        cursor.execute(sql, params)
        results = cursor.fetchall()
        print(f"Found {len(results)} results")
        for r in results[:5]:
            print(r)

if __name__ == "__main__":
    print("\n--- Testing Search (Ent 0, no filter) ---")
    test_search(0)
    print("\n--- Testing Search (Ent 0, nature='LIBRO') ---")
    test_search(0, naturaleza='LIBRO')
    print("\n--- Testing Search (Ent 0, query='Oxford') ---")
    test_search(0, query='Oxford')
