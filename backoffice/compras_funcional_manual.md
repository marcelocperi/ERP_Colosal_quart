# Guía Funcional Colosal: Módulo de Compras (Ciclo de Usuario)

Esta guía explica **cómo** utilizar el módulo de compras en términos de procesos de negocio, sin tecnicismos de sistemas.

## 1. Gestión de Proveedores y Precios
Para que el sistema sepa a quién comprarle y a qué precio, primero debemos configurar nuestros orígenes de suministro:
*   **Cargar un Proveedor**: Ingresa al maestro de Terceros y asegúrate de marcar al contacto como "Proveedor".
*   **Asignar Precios de Compra**: En la ficha del artículo, define quién es el proveedor habitual y cuál fue el último precio pactado. Esto permite que el sistema sugiera el costo automáticamente al hacer un pedido.

## 2. El Proceso de Abastecimiento (Make or Buy)
El sistema te ayuda a decidir si debes **Comprar** un insumo o **Fabricar** un producto:
*   **Si es Compra Directa**: Ve a la sección de Órdenes de Compra, selecciona al proveedor y los artículos. El sistema cargará el último precio negociado.
*   **Si es Producción (BOM)**: El sistema utiliza una "Receta". Si quieres saber cuánto te cuesta fabricar algo, el consultor te mostrará la suma de todos los ingredientes más los gastos de fábrica (luz, mano de obra, etc.).

## 3. Gestión de Consignaciones (Mercadería de Terceros)
Si trabajas con mercadería que no es tuya (consignación) o envías partes a un taller externo (fazón):
*   **Recibir en Consignación**: Registra el ingreso de la mercadería indicando que es "Tenencia". No genera deuda con el proveedor hasta que tú reportes que la usaste o la vendiste.
*   **Producción a Fazón**: Cuando envías materiales a un tercero para que arme un producto (ej. enviar tela para que hagan camisas), el sistema controla cuánta tela tiene el taller y cuántas camisas te debe devolver.

## 4. Facturación y Liquidación
*   **Cargar una Factura de Compra**: Una vez que recibes la mercadería y la factura del proveedor, ingresa al panel de Compras, busca la orden de compra previa (si existe) y carga los datos del comprobante (Punto de Venta, Número y CAE).
*   **Aviso de Venta (Consignación)**: Si vendiste algo que tenías en consignación, el sistema te pedirá generar un "Aviso de Venta". Esto dispara la liquidación para que puedas pagarle al dueño original de la mercadería.

## 5. Auditoría de Stock y Costos
*   **¿Por qué cambió el precio?**: El sistema detecta si un flete o un impuesto de importación subió y te avisará que el costo de tu producto terminado ha variado, sugiriendo actualizar tu precio de venta para no perder margen.
*   **Control de Mermas**: Si en la "Receta" dice que necesitas 1kg de material pero usaste 1.2kg, el sistema marcará ese desvío para que revises qué pasó en el proceso productivo.
