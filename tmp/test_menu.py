from app import app
from flask import g, url_for
from services.session_service import SessionDispatcher
from utils.menu_loader import filter_menu_by_permissions, load_menu_structure

with app.test_request_context('/?sid=test_sid'):
    g.sid = 'test_sid'
    menu = load_menu_structure()
    filtered = filter_menu_by_permissions(menu, ['sysadmin'])
    
    auditoria = filtered.get("AUDITORIA", {})
    for mod in auditoria.get('modules', []):
        if 'error' in mod.get('route', ''):
            print(f"Module: {mod['name']}, Route: {mod['route']}, URL: {mod.get('url')}")
