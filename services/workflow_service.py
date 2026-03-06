# -*- coding: utf-8 -*-
import json
from database import get_db_cursor

class WorkflowService:
    @staticmethod
    def get_rule_for_transaction(enterprise_id, module, amount):
        """
        Determina qué regla de workflow aplica.
        Prioriza reglas específicas de la empresa, luego reglas globales (E=0).
        """
        with get_db_cursor(dictionary=True) as cursor:
            # Buscar primero para la empresa específica, luego para la empresa 0
            cursor.execute("""
                SELECT * FROM sys_workflow_rules 
                WHERE enterprise_id IN (%s, 0) AND module = %s AND is_active = 1
                ORDER BY enterprise_id DESC, priority ASC
            """, (enterprise_id, module))
            rules = cursor.fetchall()
            
            for rule in rules:
                if rule['condition_type'] == 'AMOUNT_GTE':
                    if float(amount) >= float(rule['condition_value']):
                        return rule
                elif rule['condition_type'] == 'ALWAYS':
                    return rule
            
        return None

    @staticmethod
    def start_workflow(enterprise_id, trans_type, trans_id, amount):
        """
        Inicia una instancia de workflow para una transacción (PO, NP, etc).
        """
        rule = WorkflowService.get_rule_for_transaction(enterprise_id, 'COMPRAS', amount)
        if not rule:
            return None # No requiere workflow especial? (Aprobación simple)

        with get_db_cursor() as cursor:
            # Verificar si ya existe
            cursor.execute("""
                SELECT id FROM sys_transaction_approvals 
                WHERE enterprise_id = %s AND transaction_type = %s AND transaction_id = %s
            """, (enterprise_id, trans_type, trans_id))
            if cursor.fetchone():
                return None
            
            cursor.execute("""
                INSERT INTO sys_transaction_approvals 
                (enterprise_id, transaction_type, transaction_id, rule_id, current_step, status)
                VALUES (%s, %s, %s, %s, 1, 'PENDING')
            """, (enterprise_id, trans_type, trans_id, rule['id']))
            return cursor.lastrowid

    @staticmethod
    def get_approval_state(enterprise_id, trans_type, trans_id):
        """
        Obtiene el estado actual del workflow para una transacción.
        """
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT a.*, r.name as rule_name, s.description as step_description, 
                       s.role_id, s.step_order, (SELECT MAX(step_order) FROM sys_workflow_steps WHERE rule_id = r.id) as total_steps
                FROM sys_transaction_approvals a
                JOIN sys_workflow_rules r ON a.rule_id = r.id
                JOIN sys_workflow_steps s ON s.rule_id = r.id AND s.step_order = a.current_step
                WHERE a.enterprise_id = %s AND a.transaction_type = %s AND a.transaction_id = %s
            """, (enterprise_id, trans_type, trans_id))
            return cursor.fetchone()

    @staticmethod
    def get_workflow_history(enterprise_id, trans_type, trans_id):
        """
        Retorna el historial completo de firmas para auditoría.
        """
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT s.*, u.username, r.name as role_name, ws.description as step_name
                FROM sys_approval_signatures s
                JOIN sys_users u ON s.user_id = u.id
                LEFT JOIN sys_roles r ON u.role_id = r.id
                JOIN sys_transaction_approvals ta ON s.approval_id = ta.id
                JOIN sys_workflow_steps ws ON ta.rule_id = ws.rule_id AND s.step_order = ws.step_order
                WHERE ta.enterprise_id = %s AND ta.transaction_type = %s AND ta.transaction_id = %s
                ORDER BY s.signed_at ASC
            """, (enterprise_id, trans_type, trans_id))
            return cursor.fetchall()

    @staticmethod
    def approve_step(enterprise_id, trans_type, trans_id, user_id, role_id, comment=None):
        """
        Registra la firma de un paso y avanza el workflow si corresponde.
        Incluye generación de hash de seguridad (Standard audit).
        """
        import hashlib
        import datetime

        state = WorkflowService.get_approval_state(enterprise_id, trans_type, trans_id)
        if not state:
            return {'success': False, 'message': 'No hay un workflow activo o ya ha finalizado.'}
        
        if state['status'] != 'PENDING':
            return {'success': False, 'message': f'El workflow ya está en estado {state["status"]}.'}

        # Validar si el usuario tiene el rol requerido
        if int(state['role_id']) != int(role_id):
            return {'success': False, 'message': 'Tu rol no está autorizado para este nivel de aprobación.'}

        # Generar firma digital (Audit standard)
        data_to_sign = f"{trans_type}-{trans_id}-{user_id}-{datetime.datetime.now().isoformat()}"
        sig_hash = hashlib.sha256(data_to_sign.encode()).hexdigest()

        with get_db_cursor() as cursor:
            # 1. Registrar firma
            cursor.execute("""
                INSERT INTO sys_approval_signatures (enterprise_id, approval_id, step_order, user_id, action, comment, signature_hash)
                VALUES (%s, %s, %s, %s, 'APPROVE', %s, %s)
            """, (enterprise_id, state['id'], state['current_step'], user_id, comment, sig_hash))
            
            # 2. Verificar progreso
            cursor.execute("SELECT min_approvals FROM sys_workflow_steps WHERE rule_id = %s AND step_order = %s", 
                         (state['rule_id'], state['current_step']))
            min_req = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM sys_approval_signatures WHERE approval_id = %s AND step_order = %s AND action = 'APPROVE'",
                         (state['id'], state['current_step']))
            count = cursor.fetchone()[0]
            
            if count >= min_req:
                if state['current_step'] < state['total_steps']:
                    cursor.execute("UPDATE sys_transaction_approvals SET current_step = current_step + 1 WHERE id = %s", (state['id'],))
                    return {'success': True, 'message': 'Paso aprobado. Escalando al siguiente nivel.', 'final': False}
                else:
                    cursor.execute("UPDATE sys_transaction_approvals SET status = 'APPROVED' WHERE id = %s", (state['id'],))
                    return {'success': True, 'message': 'Aprobación final completada.', 'final': True}
            
            return {'success': True, 'message': 'Firma guardada. Se requieren más autorizaciones en este nivel.', 'final': False}
