import asyncio
import sys
import os

# Add local paths
sys.path.append(os.getcwd())

from services.afip_service import AfipService
from database import get_db_cursor

async def reprocess_invoices(invoice_ids):
    print(f"Reprocesando facturas: {invoice_ids}")
    for inv_id in invoice_ids:
        print(f"\nSolicitando CAE para ID {inv_id}...")
        try:
            # AfipService.solicitar_cae expects enterprise_id and either ID or data dict
            res = await AfipService.solicitar_cae(0, inv_id)
            if res.get('success'):
                print(f"  SUCCESS! CAE: {res['cae']}")
                # Update the DB (though solicitar_cae SHOULD have done it if successful)
                # Wait, does solicitar_cae update the DB?
                # Let's check the code of solicitar_cae further down.
            else:
                print(f"  FAILED: {res.get('error')}")
        except Exception as e:
            print(f"  EXCEPTION: {str(e)}")

if __name__ == '__main__':
    # IDs for 0001-00000005 and 0001-00000006 are 55 and 56
    asyncio.run(reprocess_invoices([55, 56]))
