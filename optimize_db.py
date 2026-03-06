import mariadb
from database import DB_CONFIG

def optimize_db():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("--- Optimizando Índices ---")
        
        # stk_existencias: optimizar búsqueda por articulo/empresa
        print("Añadiendo índice a stk_existencias...")
        try:
            cursor.execute("CREATE INDEX idx_existencias_ent_art ON stk_existencias (enterprise_id, articulo_id)")
        except Exception as e: print(f"  {e}")

        # prestamos: optimizar búsqueda de libros pendientes por empresa
        print("Añadiendo índice a prestamos...")
        try:
            cursor.execute("CREATE INDEX idx_prestamos_resumen ON prestamos (enterprise_id, libro_id, fecha_devolucion_real)")
        except Exception as e: print(f"  {e}")

        # stk_articulos: optimizar filtros frecuentes
        print("Añadiendo índices a stk_articulos...")
        try:
            cursor.execute("CREATE INDEX idx_articulos_filtros ON stk_articulos (enterprise_id, modelo, marca)")
        except Exception as e: print(f"  {e}")
        
        conn.commit()
        print("¡Optimización de DB completada!")
        conn.close()
    except Exception as e:
        print(f"Error crítico: {e}")

if __name__ == "__main__":
    optimize_db()
