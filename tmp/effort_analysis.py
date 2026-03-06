
from database import get_db_cursor

def analyze_effort():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE()
            AND (TABLE_NAME LIKE 'fin_%' 
                 OR TABLE_NAME LIKE 'cmp_%' 
                 OR TABLE_NAME LIKE 'stk_%' 
                 OR TABLE_NAME LIKE 'ven_%'
                 OR TABLE_NAME LIKE 'vta_%')
        """)
        tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
        
        modules = {
            'FINANCIERO (fin_)': {'total': 0, 'fail': 0, 'partial': 0},
            'COMPRAS (cmp_)': {'total': 0, 'fail': 0, 'partial': 0},
            'STOCK (stk_)': {'total': 0, 'fail': 0, 'partial': 0},
            'VENTAS (ven_/vta_)': {'total': 0, 'fail': 0, 'partial': 0}
        }
        
        for table in tables:
            prefix = table.split('_')[0]
            m_key = 'GENERAL'
            if prefix == 'fin': m_key = 'FINANCIERO (fin_)'
            elif prefix == 'cmp': m_key = 'COMPRAS (cmp_)'
            elif prefix == 'stk': m_key = 'STOCK (stk_)'
            elif prefix in ['ven', 'vta']: m_key = 'VENTAS (ven_/vta_)'
            
            if m_key not in modules: modules[m_key] = {'total': 0, 'fail': 0, 'partial': 0}
            
            modules[m_key]['total'] += 1
            
            cursor.execute(f"DESCRIBE {table}")
            columns = [row['Field'].lower() for row in cursor.fetchall()]
            
            has_create = any(c in columns for c in ['user_id', 'usuario_id', 'created_by', 'created_at', 'fecha_alta'])
            has_update = any(c in columns for c in ['updated_at', 'dt_date_update', 'user_id_update', 'usuario_mod'])
            
            if not has_create:
                modules[m_key]['fail'] += 1
            elif not has_update:
                modules[m_key]['partial'] += 1

        print("MODULE_DATA_START")
        import json
        print(json.dumps(modules))
        print("MODULE_DATA_END")

if __name__ == "__main__":
    analyze_effort()
