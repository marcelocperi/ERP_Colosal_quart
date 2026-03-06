from database import get_db_cursor
def run():
    with get_db_cursor(dictionary=True) as cursor:
        for tbl in ['stk_tipos_articulo', 'stk_articulos']:
            print(f'--- {tbl} ---')
            cursor.execute(f'SHOW COLUMNS FROM {tbl}')
            for r in cursor.fetchall():
                print(r['Field'])
if __name__ == '__main__':
    run()
