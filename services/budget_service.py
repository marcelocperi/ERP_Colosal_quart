# -*- coding: utf-8 -*-
from database import get_db_cursor
import datetime

class BudgetService:
    @staticmethod
    async def get_cost_centers(enterprise_id):
        """Devuelve los centros de costo activos."""
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("""
                SELECT * FROM sys_cost_centers
                WHERE enterprise_id IN (%s, 0) AND is_active = 1
                ORDER BY name ASC
            """, (enterprise_id,))
            return await cursor.fetchall()

    @staticmethod
    async def get_budget_status(enterprise_id, cost_center_id, year, month):
        """
        Calcula el estado del presupuesto para un mes específico.
        Si month = 0, es presupuesto anual.
        Retorna: {allocated, committed, actual, available}
        """
        async with get_db_cursor(dictionary=True) as cursor:
            # Primero buscamos si hay presupuesto asignado
            await cursor.execute("""
                SELECT id as budget_id, amount_allocated 
                FROM sys_budgets
                WHERE enterprise_id = %s AND cost_center_id = %s AND year = %s AND month = %s AND status = 'ACTIVE'
            """, (enterprise_id, cost_center_id, year, month))
            
            budget = await cursor.fetchone()
            if not budget:
                # Fallback a global (Empresa 0) para test, o retornar 0
                await cursor.execute("""
                    SELECT id as budget_id, amount_allocated 
                    FROM sys_budgets
                    WHERE enterprise_id = 0 AND cost_center_id = %s AND year = %s AND month = %s AND status = 'ACTIVE'
                """, (cost_center_id, year, month))
                budget = await cursor.fetchone()
                
            if not budget:
                return {'allocated': 0.0, 'committed': 0.0, 'actual': 0.0, 'available': 0.0, 'budget_id': None}

            budget_id = budget['budget_id']
            amount_allocated = float(budget['amount_allocated'])

            # Calcular ejecutado/comprometido
            await cursor.execute("""
                SELECT SUM(amount_committed) as total_committed, SUM(amount_actual) as total_actual
                FROM sys_budget_execution
                WHERE budget_id = %s
            """, (budget_id,))
            
            exec_data = await cursor.fetchone()
            total_committed = float(exec_data.get('total_committed') or 0.0)
            total_actual = float(exec_data.get('total_actual') or 0.0)

            # Disclaimer: Un gasto "Real" usualmente reemplaza un "Comprometido", 
            # pero aquí lo sumamos o calculamos Disponible = Asignado - Max(Comprometido, Real)
            # Para simplificar: Disponible = Asignado - Comprometido - Real (asumiendo transición directa o lógica de limpieza)
            # En ERP modernos, cuando un comprometido pasa a real, el comprometido disminuye.
            
            return {
                'budget_id': budget_id,
                'allocated': amount_allocated,
                'committed': total_committed,
                'actual': total_actual,
                'available': amount_allocated - (total_committed + total_actual)
            }

    @staticmethod
    async def check_funds_for_po(enterprise_id, cost_center_id, amount):
        """
        Verifica si hay fondos para aprobar una Orden de Compra.
        """
        now = datetime.datetime.now()
        status = await BudgetService.get_budget_status(enterprise_id, cost_center_id, now.year, now.month)
        
        if not status['budget_id']:
            return {'success': False, 'message': 'No hay un presupuesto asignado para este centro de costos en este mes.'}
            
        if status['available'] >= float(amount):
            return {'success': True, 'budget_id': status['budget_id']}
        else:
            return {
                'success': False, 
                'message': f"Presupuesto superado. Disponible: ${status['available']:.2f}, Solicitado: ${amount:.2f}",
                'available': status['available']
            }

    @staticmethod
    async def commit_funds(enterprise_id, transaction_type, transaction_id, cost_center_id, amount):
        """
        Reserva o 'compromete' dinero cuando se aprueba una OC.
        """
        now = datetime.datetime.now()
        status = await BudgetService.get_budget_status(enterprise_id, cost_center_id, now.year, now.month)
        
        if not status['budget_id']:
            raise Exception("No hay presupuesto definido para realizar el cargo.")
            
        async with get_db_cursor() as cursor:
            # Check if execution record already exists to avoid double charging
            await cursor.execute("""
                SELECT id FROM sys_budget_execution 
                WHERE budget_id = %s AND transaction_type = %s AND transaction_id = %s
            """, (status['budget_id'], transaction_type, transaction_id))
            if await cursor.fetchone():
                return True # Ya estaba comprometido
            
            await cursor.execute("""
                INSERT INTO sys_budget_execution (enterprise_id, budget_id, transaction_type, transaction_id, amount_committed, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (enterprise_id, status['budget_id'], transaction_type, transaction_id, amount, f"Compromiso por {transaction_type} #{transaction_id}"))
            
        return True
