from services.billing_service import BillingService

print(BillingService.get_allowed_comprobantes('Responsable Monotributo', 'Responsable Inscripto'))
print(BillingService.get_allowed_comprobantes('Monotributo', 'Consumidor Final'))
print(BillingService.get_allowed_comprobantes('Responsable Inscripto', 'Responsable Inscripto'))
