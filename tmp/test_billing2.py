from services.billing_service import BillingService
from database import get_db_cursor
import sys

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT condicion_iva FROM sys_enterprises WHERE id=0')
    emp=cursor.fetchone()
    print("Condicion IVA BD para 0:", emp['condicion_iva'])
    print("Allowed docs:", BillingService.get_allowed_comprobantes(emp['condicion_iva'], 'Responsable Inscripto'))
