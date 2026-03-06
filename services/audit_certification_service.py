
import os
import re
from database import get_db_cursor

class AuditCertificationService:
    REQUIRED_FIELDS = ['user_id', 'created_at', 'user_id_update', 'updated_at']
    
    # Mapping of module prefixes to their root directories
    MODULE_MAP = {
        'fin': 'fondos',
        'cmp': 'compras',
        'stk': 'stock',
        'vta': 'ventas',
        'erp': 'core',
        'cont': 'contabilidad',
        'ven': 'ventas',
        'vta': 'ventas'
    }
    
    CATEGORY_TO_PREFIX = {
        'CONTABILIDAD': 'cont',
        'COMPRA': 'cmp',
        'VENTA': 'vta',
        'FONDOS': 'fin',
        'STOCK': 'stk',
        'CONFIGURACION': 'erp',
        'SISTEMA': 'erp',
        'BIBLIOTECA': 'stk'
    }

    @staticmethod
    def analyze_module_compliance(module_prefix):
        """
        Analiza el cumplimiento de estándares de auditoría para un módulo específico.
        Identifica tablas, verifica esquemas y escanea el código fuente.
        """
        results = {
            'module': module_prefix,
            'tables': [],
            'code_compliance': [],
            'score': 0,
            'status': 'FAIL'
        }

        with get_db_cursor(dictionary=True) as cursor:
            # 1. Identificar Tablas del Módulo
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME LIKE %s
            """, (f"{module_prefix}_%",))
            tables = [r['TABLE_NAME'] for r in cursor.fetchall()]

            for table in tables:
                table_info = {'name': table, 'fields': {}, 'compliant': False}
                cursor.execute(f"DESCRIBE {table}")
                columns = {c['Field']: c for c in cursor.fetchall()}
                
                missing = []
                for f in AuditCertificationService.REQUIRED_FIELDS:
                    if f in columns:
                        table_info['fields'][f] = True
                    else:
                        table_info['fields'][f] = False
                        missing.append(f)
                
                table_info['compliant'] = len(missing) == 0
                table_info['missing'] = missing
                results['tables'].append(table_info)

            # 2. Análisis Estático de Código
            # Buscar en el directorio correspondiente al módulo
            module_dir = AuditCertificationService.MODULE_MAP.get(module_prefix)
            if module_dir:
                search_paths = [
                    os.path.join(os.getcwd(), module_dir),
                    os.path.join(os.getcwd(), 'services')
                ]
                
                for table in tables:
                    code_info = {'table': table, 'insert_audit': False, 'update_audit': False, 'files': []}
                    
                    for path in search_paths:
                        if not os.path.exists(path): continue
                        
                        for root, _, files in os.walk(path):
                            for file in files:
                                if file.endswith('.py'):
                                    file_path = os.path.join(root, file)
                                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                        content = f.read()
                                        if table in content:
                                            # Regex para buscar INSERT que incluya user_id
                                            if re.search(rf"INSERT\s+INTO\s+{table}.*user_id", content, re.IGNORECASE | re.DOTALL):
                                                code_info['insert_audit'] = True
                                            
                                            # Regex para buscar UPDATE que incluya user_id_update
                                            if re.search(rf"UPDATE\s+{table}.*SET.*user_id_update", content, re.IGNORECASE | re.DOTALL):
                                                code_info['update_audit'] = True
                                                
                                            code_info['files'].append(os.path.relpath(file_path, os.getcwd()))

                    results['code_compliance'].append(code_info)

        # Calcular Score
        total_tables = len(results['tables'])
        if total_tables > 0:
            compliant_tables = len([t for t in results['tables'] if t['compliant']])
            results['score'] = (compliant_tables / total_tables) * 100
            if results['score'] == 100:
                results['status'] = 'CERTIFIED'
            elif results['score'] > 70:
                results['status'] = 'WARNING'
        
        return results

    @staticmethod
    def get_all_modules_compliance():
        modules = ['fin', 'cmp', 'stk', 'vta', 'erp', 'cont']
        return [AuditCertificationService.analyze_module_compliance(m) for m in modules]

    @staticmethod
    def validate_permissions_compliance(permission_ids):
        """
        Verifica si los módulos asociados a una lista de permisos cumplen con el estándar.
        Retorna (True, None) o (False, error_details).
        """
        if not permission_ids:
            return True, None
            
        with get_db_cursor(dictionary=True) as cursor:
            # Obtener categorías únicas involucradas
            format_strings = ','.join(['%s'] * len(permission_ids))
            cursor.execute(f"SELECT DISTINCT category FROM sys_permissions WHERE id IN ({format_strings})", tuple(permission_ids))
            categories = [r['category'] for r in cursor.fetchall() if r['category']]
            
            non_compliant = []
            for cat in categories:
                prefix = AuditCertificationService.CATEGORY_TO_PREFIX.get(cat.upper())
                if prefix:
                    compliance = AuditCertificationService.analyze_module_compliance(prefix)
                    if compliance['status'] == 'FAIL':
                        non_compliant.append({
                            'module': cat,
                            'score': compliance['score'],
                            'details': "Falta de campos obligatorios (user_id/date) o falta de soporte en código."
                        })
            
            if non_compliant:
                return False, non_compliant
                
        return True, None

    @staticmethod
    def notify_saas_owner(violation_details, actor_user, target_object):
        """
        Envía una alerta crítica al SaaS Owner sobre la violación de estándares de auditoría.
        """
        from services.email_service import _enviar_email, _generar_html_template
        
        # El email del SaaS Owner suele ser el del superadmin o el de la empresa ID 1
        saas_owner_email = "marcelo_peri@yahoo.com" 
        subject = "ALERTA CRÍTICA: Intento de Operación en Módulo No Certificado"
        
        detalles = {
            "Inspector": actor_user,
            "Objeto Afectado": target_object,
            "Fecha/Hora": "Inmediata",
            "Criticidad": "BLOQUEANTE (SOX/CISA Violation)"
        }
        
        # Formatear el detalle de la violación
        violaciones_html = "<ul>"
        for v in violation_details:
            violaciones_html += f"<li><strong>Módulo {v['module']}:</strong> Score {v['score']:.1f}% - {v['details']}</li>"
        violaciones_html += "</ul>"
        
        mensaje = f"""
        Se ha bloqueado un intento de grabación/asignación en el sistema debido a que los módulos involucrados 
        <strong>NO cumplen con los estándares de trazabilidad exigidos para auditoría</strong>.
        <br><br>
        <strong>Detalle del Bloqueo:</strong>
        {violaciones_html}
        <br><br>
        La transacción ha sido anulada automáticamente para preservar la integridad del log de auditoría global.
        """
        
        html = _generar_html_template(
            "Violación de Estándar de Auditoría",
            mensaje,
            detalles,
            color_primario="#b91c1c" # Rojo crítico
        )
        
        return _enviar_email(saas_owner_email, subject, html)
