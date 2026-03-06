from database import get_db_cursor
import json
import traceback
import time

with get_db_cursor() as check:
    check.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
    has_clob = bool(check.fetchone())
            
with get_db_cursor() as cursor:
    # Generar algunos errores de prueba
    clob_404 = {
        'request_path': '/compras/inventario-no-existe',
        'referrer': 'http://localhost:5005/compras/dashboard',
        'traceback': 'File not found on the server'
    }
    
    clob_500 = {
        'request_path': '/ventas/facturar',
        'referrer': 'http://localhost:5005/ventas/dashboard',
        'traceback': 'ValueError: No active tax profile for the selected user.\n  File \"/app.py\", line 45, in some_function\n    raise ValueError()'
    }
    
    clob_data_col = 'clob_data' if has_clob else 'error_traceback'
    
    # 1. Error de 404
    cursor.execute(f"""
        INSERT INTO sys_transaction_logs 
        (enterprise_id, user_id, module, endpoint, request_method, request_data, 
         status, severity, impact_category, failure_mode, error_message, {clob_data_col})
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (1, 1, 'SYSTEM', '/compras/inventario-no-existe', 'GET', '{}',
          'ERROR', 3, 'OPERACIONAL', 'HTTP_404', 'URL Not Found (404)', json.dumps(clob_404)))
          
    # 2. Error 500 logico
    cursor.execute(f"""
        INSERT INTO sys_transaction_logs 
        (enterprise_id, user_id, module, endpoint, request_method, request_data, 
         status, severity, impact_category, failure_mode, error_message, {clob_data_col})
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (1, 1, 'VENTAS', '/ventas/facturar', 'POST', '{\"total\": 500}',
          'ERROR', 8, 'FINANCIERO', 'BUSINESS_LOGIC', 'No active tax profile for the selected user.', json.dumps(clob_500)))

    # 3. Validation error
    cursor.execute(f"""
        INSERT INTO sys_transaction_logs 
        (enterprise_id, user_id, module, endpoint, request_method, request_data, 
         status, severity, impact_category, failure_mode, error_message, {clob_data_col})
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (0, 1, 'AUTH', '/login', 'POST', '{\"user\": \"admin\"}',
          'ERROR', 5, 'REPUTACIONAL', 'SECURITY_AUTH', 'Multiple failed auth attempts detected.', json.dumps({'IP': '192.168.1.55'})))
          
print("Insercion exitosa de datos de prueba en la tabla sys_transaction_logs")
