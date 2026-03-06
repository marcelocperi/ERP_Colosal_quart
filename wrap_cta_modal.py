import os

path = 'ventas/templates/ventas/perfil_cliente.html'
with open(path, 'r', encoding='utf-8') as f:
    html = f.read()

# Start of Cuenta Corriente
start_idx = html.find('<!-- CUENTA CORRIENTE -->')

if start_idx != -1:
    cta_cte_html = html[start_idx:html.rfind('{% endblock %}')].strip()
    
    # Let's rebuild the template with the modal wrapper
    modal_wrapper = """
<!-- MODAL DE CUENTA CORRIENTE AUTOMATICO -->
<div class="modal fade" id="modalCtaCteFicha" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content bg-dark text-light border-0">
            <div class="modal-header border-bottom border-secondary">
                <h5 class="modal-title">
                    <i class="fas fa-file-invoice-dollar text-primary mr-2"></i>Cuenta Corriente: <span class="fw-bold">{{ cliente.nombre }}</span> 
                </h5>
                <button type="button" class="close text-white" data-dismiss="modal" aria-label="Close">
                    <span aria-hidden="true">&times;</span>
                </button>
            </div>
            <div class="modal-body">
                [CTA_CTE_CONTENT]
            </div>
            <div class="modal-footer border-top border-secondary">
                <button type="button" class="btn btn-secondary" data-dismiss="modal">Cerrar</button>
            </div>
        </div>
    </div>
</div>

<script>
    document.addEventListener('DOMContentLoaded', function() {
        // Mostrar el modal automáticamente al entrar a la ficha
        $('#modalCtaCteFicha').modal('show');
    });
</script>
"""
    # Remove the original h5
    import re
    # Remove the <h5> title block because the modal has its own title, 
    # but keep the saldo part if we want, actually let's just put everything inside the modal body
    
    final_modal = modal_wrapper.replace('[CTA_CTE_CONTENT]', cta_cte_html)
    
    new_html = html[:start_idx] + final_modal + '\n{% endblock %}\n'
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(new_html)
    
    print("Cuenta corriente envuelta en modal exitosamente.")
else:
    print("No se encontró la seccion CUENTA CORRIENTE")
