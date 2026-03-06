
content = """{% extends "base.html" %}

{% block content %}
<div class="container-fluid mt-4">
    <div class="row">
        <div class="col-md-11 mx-auto">
            <div class="card shadow-lg border-0 bg-dark text-light overflow-hidden">
                <div class="card-header bg-gradient-primary text-white p-4 d-flex justify-content-between align-items-center">
                    <div>
                        <h3 class="mb-0"><i class="fas fa-undo-alt me-2"></i>Solicitud de Devolución</h3>
                        <p class="mb-0 opacity-75 small uppercase">Referencia Venta: Factura #{{ "%04d-%08d"|format(factura.punto_venta, factura.numero) }}</p>
                    </div>
                </div>

                <div class="card-body p-4">
                    <form id="devolucionForm">
                        <div class="row g-4 mb-5">
                            <div class="col-md-3">
                                <label class="form-label text-info small uppercase fw-bold">Cliente</label>
                                <input type="text" class="form-control bg-dark text-muted border-secondary" value="{{ factura.tercero_nombre }}" readonly>
                                <input type="hidden" id="cliente_id" value="{{ factura.tercero_id }}">
                            </div>
                            <div class="col-md-3">
                                <label class="form-label text-info small uppercase fw-bold">Depósito de Recepción</label>
                                <select class="form-select bg-dark text-light border-info" id="deposito_id" required>
                                    <option value="">Seleccione depósito...</option>
                                    {% for d in depositos %}
                                    <option value="{{ d.id }}" {{ 'selected' if d.id == deposito_sugerido }}>{{ d.nombre }}</option>
                                    {% endfor %}
                                </select>
                            </div>
                            <div class="col-md-3">
                                <label class="form-label text-info small uppercase fw-bold">Logística</label>
                                <select class="form-select bg-dark text-light border-info" id="logistica_id">
                                    <option value="">Consumidor Final / Propia</option>
                                    {% for t in transportistas %}
                                    <option value="{{ t.id }}">{{ t.nombre }}</option>
                                    {% endfor %}
                                </select>
                            </div>
                            <div class="col-md-3">
                                <label class="form-label text-info small uppercase fw-bold">Condición de Devolución</label>
                                <select class="form-select bg-dark text-light border-info" id="condicion_pago_id" required>
                                    <option value="">Seleccione condición...</option>
                                    {% for cp in condiciones %}
                                    <option value="{{ cp.id }}">{{ cp.nombre }}</option>
                                    {% endfor %}
                                </select>
                            </div>
                        </div>

                        <div class="table-responsive rounded border border-secondary shadow-sm mb-5">
                            <table class="table table-dark table-hover mb-0" id="itemsTable">
                                <thead class="bg-secondary bg-opacity-25 text-muted small uppercase">
                                    <tr>
                                        <th class="ps-4">Código / Descripción</th>
                                        <th class="text-end">Cant. Original</th>
                                        <th class="text-center" style="width: 150px;">Cant. A Devolver</th>
                                        <th class="text-end">Precio Unit.</th>
                                        <th class="text-end">IVA %</th>
                                        <th class="text-end pe-4">Subtotal</th>
                                        <th></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for item in items_precargados %}
                                    <tr class="align-middle" data-id="{{ item.id }}" data-iva="{{ item.iva }}">
                                        <td class="ps-4"><b>{{ item.nombre }}</b></td>
                                        <td class="text-end text-muted">{{ item.cantidad }}</td>
                                        <td class="text-center">
                                            <input type="number" class="form-control bg-dark text-light text-center border-primary cant-devolver" value="{{ item.cantidad }}" min="0" max="{{ item.cantidad }}" step="0.0001" onchange="recalcularTotales()">
                                        </td>
                                        <td class="text-end">{{ item.precio }}</td>
                                        <td class="text-end">{{ item.iva }}%</td>
                                        <td class="text-end fw-bold text-info pe-4 subtotal-fila">$ 0.00</td>
                                        <td>
                                            <button type="button" class="btn btn-sm btn-outline-danger border-0" onclick="this.closest('tr').remove(); recalcularTotales();">
                                                <i class="fas fa-times"></i>
                                            </button>
                                        </td>
                                    </tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>

                        <div class="row g-4">
                            <div class="col-md-7">
                                <h5 class="text-info mb-3">Sugerencia de Reembolso</h5>
                                <div class="bg-secondary bg-opacity-10 p-3 rounded border border-secondary">
                                    <div class="list-group list-group-flush bg-transparent">
                                        {% for pago in pagos_originales %}
                                        <div class="list-group-item bg-transparent text-light border-secondary d-flex justify-content-between align-items-center py-3">
                                            <div>
                                                <b>{{ pago.medio_nombre }}</b>
                                                <div class="small text-muted">Original: $ {{ pago.importe }}</div>
                                            </div>
                                            <div class="form-check form-switch">
                                                <input class="form-check-input switch-reembolso" type="checkbox" data-medio="{{ pago.medio_pago_id }}" data-importe="{{ pago.importe }}" checked>
                                                <label class="form-check-label small">Reembolsar</label>
                                            </div>
                                        </div>
                                        {% endfor %}
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-5">
                                <div class="bg-primary bg-opacity-10 p-4 rounded border border-primary">
                                    <div class="d-flex justify-content-between mb-2">
                                        <span>Subtotal Neto</span><span id="txtNeto">$ 0.00</span>
                                    </div>
                                    <div class="d-flex justify-content-between mb-2 pb-2 border-bottom">
                                        <span>IVA</span><span id="txtIva">$ 0.00</span>
                                    </div>
                                    <div class="d-flex justify-content-between align-items-center mt-3">
                                        <h4>Total NC</h4><h2 class="text-primary" id="txtTotal">$ 0.00</h2>
                                    </div>
                                    <textarea id="observaciones" class="form-control bg-dark text-light border-primary mt-4" rows="3" placeholder="Observaciones..."></textarea>
                                    <button type="submit" class="btn btn-primary btn-lg w-100 mt-4 shadow" id="btnSolicitar">
                                        <i class="fas fa-paper-plane me-2"></i>Enviar Solicitud
                                    </button>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </div>
</div>

<script>
    function recalcularTotales() {
        let neto = 0; let iva = 0;
        document.querySelectorAll('#itemsTable tbody tr').forEach(tr => {
            const cant = parseFloat(tr.querySelector('.cant-devolver').value) || 0;
            const precio = parseFloat(tr.cells[3].innerText);
            const ivaAlic = parseFloat(tr.dataset.iva);
            const s = cant * precio;
            const i = s * (ivaAlic / 100);
            neto += s; iva += i;
            tr.querySelector('.subtotal-fila').innerText = '$ ' + (s + i).toFixed(2);
        });
        document.getElementById('txtNeto').innerText = '$ ' + neto.toFixed(2);
        document.getElementById('txtIva').innerText = '$ ' + iva.toFixed(2);
        document.getElementById('txtTotal').innerText = '$ ' + (neto + iva).toFixed(2);
    }
    document.addEventListener('DOMContentLoaded', recalcularTotales);

    document.getElementById('devolucionForm').addEventListener('submit', async function (e) {
        e.preventDefault();
        const items = [];
        document.querySelectorAll('#itemsTable tbody tr').forEach(tr => {
            const cant = parseFloat(tr.querySelector('.cant-devolver').value);
            if (cant > 0) {
                items.push({
                    articulo_id: tr.dataset.id,
                    cantidad: cant,
                    precio: parseFloat(tr.cells[3].innerText),
                    iva: parseFloat(tr.dataset.iva)
                });
            }
        });
        if (items.length === 0) return alert('Seleccione artículos');

        const reembolsos = [];
        document.querySelectorAll('.switch-reembolso:checked').forEach(sw => {
            reembolsos.push({ medio_id: sw.dataset.medio, importe: parseFloat(sw.dataset.importe) });
        });

        const payload = {
            factura_id: {{ factura.id }},
            cliente_id: document.getElementById('cliente_id').value,
            deposito_id: document.getElementById('deposito_id').value,
            logistica_id: document.getElementById('logistica_id').value,
            condicion_pago_id: document.getElementById('condicion_pago_id').value,
            observaciones: document.getElementById('observaciones').value,
            items: items,
            reembolsos: reembolsos
        };

        const csrfToken = document.querySelector('meta[name="csrf-token"]').content;
        try {
            const res = await fetch("/ventas/devolucion-solicitar" + (window.SID ? "?sid="+window.SID : ""), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            if (data.success) {
                alert('Solicitud enviada');
                window.location.href = "{{ url_for('ventas.comprobantes') }}?sid=" + (window.SID || '');
            } else alert('Error: ' + data.message);
        } catch (err) { alert('Error: ' + err.message); }
    });
</script>
{% endblock %}"""

with open('ventas/templates/ventas/devolucion_solicitud.html', 'w', encoding='utf-8') as f:
    f.write(content)
