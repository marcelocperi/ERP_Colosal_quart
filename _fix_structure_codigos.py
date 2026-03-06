from database import get_db_cursor

def fix_structure():
    print("🛠️ Corrigiendo estructura de stk_articulos_codigos...")
    with get_db_cursor() as cursor:
        # Renombrar columna codigo_barras a codigo
        try:
            cursor.execute("ALTER TABLE stk_articulos_codigos CHANGE COLUMN codigo_barras codigo VARCHAR(100) NOT NULL")
            print("✅ Columna 'codigo_barras' renombrada a 'codigo'.")
        except Exception as e:
            if "Unknown column" in str(e):
                print("⚠️ La columna 'codigo_barras' no existe. Verificando si ya se llama 'codigo'...")
            else:
                print(f"❌ Error al renombrar columna: {e}")

        # Asegurar que los indices sean correctos
        try:
            cursor.execute("ALTER TABLE stk_articulos_codigos DROP INDEX IF EXISTS enterprise_id")
            cursor.execute("ALTER TABLE stk_articulos_codigos ADD UNIQUE INDEX unq_ent_codigo (enterprise_id, codigo)")
            print("✅ Índice único actualizado.")
        except Exception as e:
            print(f"⚠️ Nota sobre índices: {e}")

if __name__ == "__main__":
    fix_structure()
