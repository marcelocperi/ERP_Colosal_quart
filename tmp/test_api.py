from flask import Flask, g, request, jsonify
from ventas.routes import get_allowed_docs
from app import app
from database import get_db_cursor
from services.billing_service import BillingService

with app.test_request_context('/api/ventas/fiscal/allowed-docs?tipo_responsable=Responsable+Inscripto'):
    g.user = {'enterprise_id': 0}
    
    receptor_tipo = request.args.get('tipo_responsable', '*')
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT condicion_iva FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        emp = cursor.fetchone()
        emisor_tipo = emp['condicion_iva'] if emp else 'Responsable Inscripto'
        print("Emisor Tipo DB:", emisor_tipo)
        
    allowed_codigos = BillingService.get_allowed_comprobantes(emisor_tipo, receptor_tipo)
    print("Allowed codigos:", allowed_codigos)
    
    if not allowed_codigos:
        allowed_codigos = ['006', '007', '008']
        print("Fallback applied:", allowed_codigos)
        
    with get_db_cursor(dictionary=True) as cursor:
        placeholders = ', '.join(['%s'] * len(allowed_codigos))
        cursor.execute(f"SELECT codigo, descripcion, letra FROM sys_tipos_comprobante WHERE codigo IN ({placeholders})", tuple(allowed_codigos))
        tipos = cursor.fetchall()
        print("Tipos returns:", tipos)
