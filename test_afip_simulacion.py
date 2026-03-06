import sys
import os
import asyncio

# Añadir el path actual para importar servicios
sys.path.append(os.getcwd())

from services.afip_service import AfipService

async def test_afip_simulation():
    cuit_usuario = "20-17163443-2"
    enterprise_id = 1 # Usamos ID 1 para la prueba
    
    print(f"--- INICIANDO SIMULACIÓN AFIP PARA CUIT: {cuit_usuario} ---")
    
    # 1. Simular Consulta de Padrón
    print("\n1. Consultando Padrón A5...")
    res_padron = await AfipService.consultar_padron(enterprise_id, cuit_usuario)
    
    if res_padron['success']:
        d = res_padron['data']
        print(f"   [ÉXITO] Datos recuperados:")
        print(f"   - Razón Social: {d['razon_social']}")
        print(f"   - Condición IVA: {d['condicion_iva']}")
        print(f"   - Domicilio: {d['domicilio']}")
    else:
        print(f"   [ERROR]: {res_padron['error']}")

    # 2. Simular Autorización de Comprobante (CAE)
    # Nota: Necesitamos un comprobante_id que exista en la DB para el UPDATE,
    # pero para el test de lógica el servicio imprimirá el LOG.
    print("\n2. Simulando pedido de CAE (Comprobante ID: 9999)...")
    res_cae = await AfipService.solicitar_cae(enterprise_id, 9999)
    
    if res_cae['success']:
        print(f"   [ÉXITO] CAE Obtenido: {res_cae['cae']}")
        print(f"   - Vencimiento: {res_cae['vencimiento']}")
        print(f"   - Mensaje: {res_cae['mensaje']}")
    else:
        print(f"   [INFO] (Esperado para ID inexistente): {res_cae['error']}")

if __name__ == "__main__":
    asyncio.run(test_afip_simulation())
