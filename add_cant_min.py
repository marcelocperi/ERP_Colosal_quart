import sys
sys.path.append('.')
from database import get_db_cursor

try:
    with get_db_cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM stk_articulos LIKE 'cant_min_pedido'")
        col = cursor.fetchone()
        if not col:
            print('Adding cant_min_pedido column...')
            cursor.execute('ALTER TABLE stk_articulos ADD COLUMN cant_min_pedido INT DEFAULT 1')
            print('Column added.')
        else:
            print('Column already exists.')
except Exception as e:
    print('Error:', e)
