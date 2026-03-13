import asyncio
from database import get_db_cursor

async def main():
    try:
        async with get_db_cursor() as cursor:
            print("--- erp_comprobantes ---")
            await cursor.execute("DESCRIBE erp_comprobantes")
            rows = await cursor.fetchall()
            for r in rows: print(r)
            
            print("\n--- fin_ordenes_pago ---")
            await cursor.execute("DESCRIBE fin_ordenes_pago")
            rows = await cursor.fetchall()
            for r in rows: print(r)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
