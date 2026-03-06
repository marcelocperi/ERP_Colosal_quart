import sys

path = 'ventas/templates/ventas/clientes.html'
with open(path, 'r', encoding='utf-8') as f:
    html = f.read()

# 1. ADD HYPERLINK TO CODIGO
# currently: <span class="badge badge-code">{{ c.codigo or '-' }}</span>
hyperlink = '''<a href="javascript:void(0)" onclick="mostrarCuentaCorriente({{ c.id }}, '{{ c.nombre }}', '{{ c.cuit }}')" class="badge badge-code text-decoration-none" title="Ver saldos / Cuenta Corriente">{{ c.codigo or '-' }}</a>'''
html = html.replace('<span class="badge badge-code">{{ c.codigo or \'-\' }}</span>', hyperlink)

# 2. ADD MODAL HTML & JS SCRIPT AT THE BOTTOM, JUST BEFORE {% endblock %}
modal_html = '''
<!-- MODAL CUENTA CORRIENTE -->
<div class="modal fade" id="modalCuentaCorriente" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content bg-dark text-light border-0">
            <div class="modal-header border-bottom border-secondary">
                <h5 class="modal-title">
                    <i class="fas fa-file-invoice-dollar text-primary mr-2"></i>Cuenta Corriente: <span id="modalClienteNombre" class="fw-bold"></span> 
                    <small class="text-muted ml-2">CUIT: <span id="modalClienteCuit"></span></small>
                </h5>
                <button type="button" class="close text-white" data-dismiss="modal" aria-label="Close">
                    <span aria-hidden="true">&times;</span>
                </button>
            </div>
            <div class="modal-body" id="modalCtaCteBody">
                <div class="text-center py-5">
                    <div class="spinner-border text-primary" role="status">
                        <span class="sr-only">Cargando...</span>
                    </div>
                    <p class="mt-2 text-muted">Obteniendo movimientos...</p>
                </div>
            </div>
            <div class="modal-footer border-top border-secondary">
                <button type="button" class="btn btn-secondary" data-dismiss="modal">Cerrar</button>
                <a href="#" id="modalBtnPerfil" class="btn btn-primary"><i class="fas fa-id-card mr-1"></i> Ir al Perfil Completo</a>
            </div>
        </div>
    </div>
</div>

<script>
    function mostrarCuentaCorriente(cliente_id, nombre, cuit) {
        document.getElementById('modalClienteNombre').textContent = nombre;
        document.getElementById('modalClienteCuit').textContent = cuit;
        document.getElementById('modalBtnPerfil').href = "{{ url_for('ventas.perfil_cliente', id=0) }}".replace('0', cliente_id);
        
        const body = document.getElementById('modalCtaCteBody');
        body.innerHTML = '<div class="text-center py-5"><div class="spinner-border text-primary" role="status"><span class="sr-only">Cargando...</span></div><p class="mt-2 text-muted">Obteniendo movimientos...</p></div>';
        
        $('#modalCuentaCorriente').modal('show');
        
        // Fetch new data
        fetch(`/ventas/api/clientes/${cliente_id}/cuenta_corriente`)
            .then(r => r.text())
            .then(html => {
                body.innerHTML = html;
            })
            .catch(err => {
                console.error(err);
                body.innerHTML = '<div class="alert alert-danger">Error al cargar la cuenta corriente. Verifique su conexión y reintente.</div>';
            });
    }
</script>
'''

if 'modalCuentaCorriente' not in html:
    html = html.replace('{% endblock %}', modal_html + '\n{% endblock %}')

with open(path, 'w', encoding='utf-8') as f:
    f.write(html)

print("Modal de cuenta corriente integrado en clientes.html")
