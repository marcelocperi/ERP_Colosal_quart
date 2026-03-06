import os, re

filepath = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\ventas\templates\ventas\perfil_cliente.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

start_match = re.search(r'<div class="row">[\s\n]*<!-- Sidebar Resumen -->', content)
end_match = re.search(r'<!-- SEDES / DIRECCIONES -->[\s\n]*<div class="tab-pane fade show active" id="sedes" role="tabpanel">', content)

if start_match and end_match:
    new_html = r'''<!-- ESTILOS PARA TABS PREMIUM -->
<style>
    .customer-tabs {
        border-bottom: 2px solid #e3e6f0;
        margin-bottom: -2px;
        gap: 0.5rem;
    }
    .customer-tabs .nav-item .nav-link {
        color: #858796;
        font-weight: 600;
        border: none;
        background: transparent;
        padding: 1rem 1.25rem;
        border-bottom: 3px solid transparent;
        transition: all 0.2s ease-in-out;
        border-radius: 0;
    }
    .customer-tabs .nav-item .nav-link:hover {
        color: #4e73df;
        background: rgba(78, 115, 223, 0.05);
    }
    .customer-tabs .nav-item .nav-link.active {
        color: #4e73df !important;
        border-bottom: 3px solid #4e73df;
        background: transparent;
    }
    .customer-tabs .nav-item .nav-link i {
        opacity: 0.7;
        transition: opacity 0.2s;
    }
    .customer-tabs .nav-item .nav-link.active i {
        opacity: 1;
    }
</style>

<div class="row w-100 mx-0">
    <div class="col-12 px-0">
        <div class="card shadow-lg mb-4 border-0">
            <div class="card-header bg-white pb-0 pt-3 px-4 border-bottom-0">
                <ul class="nav nav-tabs customer-tabs ms-0 me-0" id="clienteTab" role="tablist">
                    <li class="nav-item">
                        <button class="nav-link active" id="ficha-tab" data-bs-toggle="tab" data-bs-target="#ficha"
                            type="button" role="tab"><i class="fas fa-id-card me-2"></i>Ficha de Cliente</button>
                    </li>
                    <li class="nav-item">
                        <button class="nav-link" id="sedes-tab" data-bs-toggle="tab" data-bs-target="#sedes"
                            type="button" role="tab"><i class="fas fa-map-marker-alt me-2"></i>Sedes/Depósitos</button>
                    </li>
                    <li class="nav-item">
                        <button class="nav-link" id="contactos-tab" data-bs-toggle="tab" data-bs-target="#contactos"
                            type="button" role="tab"><i class="fas fa-users me-2"></i>Contactos</button>
                    </li>
                    <li class="nav-item">
                        <button class="nav-link" id="fiscal-tab" data-bs-toggle="tab" data-bs-target="#fiscal"
                            type="button" role="tab"><i class="fas fa-file-invoice-dollar me-2"></i>Impuestos/Fiscal</button>
                    </li>
                    <li class="nav-item">
                        <button class="nav-link" id="cm05-tab" data-bs-toggle="tab" data-bs-target="#cm05" type="button"
                            role="tab" title="Convenio Multilateral"><i class="fas fa-globe-americas me-2"></i>CM05 / Conv. Multi.</button>
                    </li>
                    <li class="nav-item">
                        <button class="nav-link" id="pago-tab" data-bs-toggle="tab" data-bs-target="#pago" type="button"
                            role="tab"><i class="fas fa-handshake me-2"></i>Habilitaciones Pago</button>
                    </li>
                    <li class="nav-item">
                        <button class="nav-link" id="ctacte-tab" data-bs-toggle="tab" data-bs-target="#ctacte"
                            type="button" role="tab"><i class="fas fa-money-check-alt me-2"></i>Cuenta Corriente</button>
                    </li>
                </ul>
            </div>
            
            <div class="card-body bg-light p-4 rounded-bottom">
                <div class="tab-content" id="clienteTabContent">
                    <!-- FICHA DE CLIENTE -->
                    <div class="tab-pane fade show active" id="ficha" role="tabpanel">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="card shadow-sm mb-4 border-left-primary h-100">
                                    <div class="card-header py-3 bg-white border-bottom-0">
                                        <h6 class="m-0 font-weight-bold text-primary"><i class="fas fa-info-circle me-2"></i>Datos Básicos</h6>
                                    </div>
                                    <div class="card-body">
                                        <p><strong>CUIT/DNI:</strong> {{ cliente.cuit }}</p>
                                        <p><strong>Email Principal:</strong> {{ cliente.email or 'N/D' }}</p>
                                        <p><strong>Teléfono Principal:</strong> {{ cliente.telefono or 'N/D' }}</p>
                                        <hr>
                                        <p class="mb-0"><strong>Observaciones:</strong><br>
                                            <span class="text-muted small">{{ cliente.observaciones or 'Sin observaciones' }}</span>
                                        </p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="card shadow-sm mb-4 border-left-info h-100">
                                    <div class="card-header py-3 bg-white border-bottom-0 d-flex justify-content-between align-items-center">
                                        <h6 class="m-0 font-weight-bold text-info"><i class="fas fa-file-contract me-2"></i>Finanzas</h6>
                                        <button class="btn btn-sm btn-circle btn-outline-info" onclick="toggleModal('modalCondicionPago')"
                                            title="Solicitar Cambio">
                                            <i class="fas fa-sync-alt"></i>
                                        </button>
                                    </div>
                                    <div class="card-body">
                                        <div class="mb-3">
                                            <label class="small text-muted mb-1 fw-bold">Condición de Pago (Simple)</label>
                                            <div class="p-2 rounded bg-light border d-flex align-items-center">
                                                <i class="fas fa-check-circle text-success me-2"></i>
                                                <span class="fw-bold">{{ pago_info.condicion_nombre or 'SIN ASIGNAR' }}</span>
                                            </div>
                                        </div>

                                        <div class="mb-3">
                                            <label class="small text-muted mb-1 fw-bold">Estructura Mixta (Maestra)</label>
                                            {% if pago_info.condicion_mixta_id %}
                                            <div class="p-2 rounded bg-info text-white border d-flex align-items-center">
                                                <i class="fas fa-layer-group me-2"></i>
                                                <span class="fw-bold">{{ pago_info.mixta_nombre }}</span>
                                            </div>
                                            {% else %}
                                            <div class="p-2 rounded bg-light border text-muted small italic">
                                                Sin estructura mixta asignada
                                            </div>
                                            {% endif %}
                                        </div>

                                        {% if pago_info.estado_aprobacion_pago == 'PENDIENTE' %}
                                        <div class="alert alert-warning p-2 mb-2">
                                            <div class="d-flex justify-content-between align-items-start">
                                                <small><i class="fas fa-clock me-1"></i> <strong>Pendiente de Aprobación</strong></small>
                                                <span class="badge bg-warning text-dark">PENDIENTE</span>
                                            </div>
                                            <div class="mt-1 small">
                                                Solicitado: <span class="fw-bold">{{ pago_info.condicion_pendiente_nombre }}</span>
                                            </div>

                                            {% if 'gerente_ventas' in g.permissions or 'admin' in g.permissions or 'all' in g.permissions %}
                                            <div class="mt-2 d-grid gap-2">
                                                <form action="{{ url_for('ventas.aprobar_condicion_pago', id=cliente.id) }}" method="POST">
                                                    <div class="btn-group w-100">
                                                        <button type="submit" name="action" value="approve"
                                                            class="btn btn-sm btn-success">Aprobar</button>
                                                        <button type="submit" name="action" value="reject"
                                                            class="btn btn-sm btn-danger">Rechazar</button>
                                                    </div>
                                                </form>
                                            </div>
                                            {% endif %}
                                        </div>
                                        {% elif pago_info.estado_aprobacion_pago == 'RECHAZADO' %}
                                        <div class="alert alert-danger p-2 mb-0 small">
                                            <i class="fas fa-times-circle me-1"></i> El último cambio fue <strong>RECHAZADO</strong>.
                                        </div>
                                        {% endif %}

                                        {% if pago_info.aprobador_nombre %}
                                        <div class="mt-3 extra-small text-muted">
                                            <i class="fas fa-user-shield me-1"></i> Aprobado por: {{ pago_info.aprobador_nombre }}<br>
                                            <i class="fas fa-calendar-alt me-1"></i> Fecha: {{ pago_info.fecha_aprobacion_pago }}
                                        </div>
                                        {% endif %}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- SEDES / DIRECCIONES -->
                    <div class="tab-pane fade" id="sedes" role="tabpanel">'''
    new_content = content[:start_match.start()] + new_html + content[end_match.end():]
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("SUCCESS")
else:
    print("MATCH NOT FOUND")
    if not start_match: print("Start not found")
    if not end_match: print("End not found")
