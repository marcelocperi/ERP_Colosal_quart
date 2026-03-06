# Manual Funcional: Módulo de Ventas y Cobranzas (Procesos de Negocio)

Este manual describe el flujo de trabajo para el usuario de ventas, enfocado en **cómo** operar el sistema sin tecnicismos de programación.

## 1. El Maestro de Clientes: Tu Base de Datos Comercial
Para venderle a un cliente por primera vez, primero dalo de alta:
*   **Alta de Cliente**: Registra el nombre, CUIT y dirección fiscal. Asegúrate de que el CUIT sea válido para que el sistema pueda facturar electrónicamente (AFIP).
*   **Gestión de Direcciones de Entrega**: Un cliente puede tener varias direcciones. Cárgalas todas para que, al facturar, el sistema te permita elegir dónde debe ir el pedido.
*   **Condiciones de Pago**: Puedes pactar que el cliente pague de contado, a 7 días, o a 30 días. Si eres gerente, puedes aprobar cambios de condición para clientes preferenciales.

## 2. Facturación Electrónica (El Ciclo de Venta)
Facturar es el corazón de la operación:
*   **Realizar una Factura**: Ve al panel de Facturación, elige al cliente y el sistema traerá su condición de pago y dirección de entrega. Agrega los artículos (puedes usar el buscador o el escáner de balanza).
*   **Letras del Comprobante (A, B, C)**: No te preocupes por el tipo (A o B); el sistema detecta automáticamente qué letra debe ser según el CUIT y la condición de IVA del cliente.
*   **Autorización AFIP (CAE)**: Al confirmar, el sistema se conecta con AFIP para obtener el CAE. Si sale bien, la factura ya es legal y se le puede enviar por mail al cliente.

## 3. Notas de Crédito e Impuestos (IIBB)
Si un cliente te devuelve mercadería:
*   **Hacer una Nota de Crédito**: Busca la factura original y selecciona qué artículos regresaron. El sistema reingresará la mercadería a tu depósito y reducirá la deuda del cliente automáticamente.
*   **Percepciones de Ingresos Brutos (IIBB)**: Si tu empresa es agente, el sistema sumará automáticamente el impuesto según la jurisdicción del cliente. Todo está configurado en "Datos Fiscales".

## 4. Cuenta Corriente y Cobranzas
Para saber cuánto dinero te debe tu cartera de clientes:
*   **Consultar el Saldo**: En el perfil del cliente verás su "Saldo de Cuenta Corriente". El saldo es la diferencia entre todas las facturas que le hiciste menos los pagos que él te entregó (Recibos).
*   **Emitir un Recibo**: Cuando el cliente te paga, genera el Recibo indicando el medio de pago (Efectivo, Cheque, Transferencia) y aplícalo a las facturas que están cancelando. Esto limpia su deuda.

## 5. Reportes de Ventas
*   **Ranking de Ventas**: Mira qué clientes te compraron más en el último mes para enfocar tus esfuerzos de cobranza o marketing.
*   **Ranking de Artículos**: ¿Qué es lo que más se está vendiendo? El sistema te muestra los productos estrella de tu catálogo.
