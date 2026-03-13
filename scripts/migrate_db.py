import re
import os

def migrate_db_to_quart():
    filepath = r'C:\Users\marce\Documents\GitHub\quart\database.py'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update imports
    content = content.replace('from flask import', 'from quart import')
    
    # 2. Make _log_transaction_error async and await request properties
    content = content.replace('def _log_transaction_error(e,', 'async def _log_transaction_error(e,')
    
    # Needs to await in inner try for request
    content = content.replace('if request.is_json: req_data = request.json', 'if request.is_json: req_data = await request.json')
    content = content.replace('elif request.form: req_data = dict(request.form)', 'elif (await request.form): req_data = dict(await request.form)')
    
    # 3. Update the decorator atomic_transaction call inside async_wrapper
    # From: _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode)
    # To: await _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode)
    content = content.replace('_log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode)',
                              'await _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode)')

    # 4. For sync_wrapper, it cannot await. In Quart, it's better if everything is async.
    # But for now, we will add a check. Actually, in Quart we *should* avoid sync_wrapper for anything touching requests.
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Migración base de database.py a Quart completada (Core).")

if __name__ == "__main__":
    migrate_db_to_quart()
