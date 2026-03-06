import sys
path = 'ventas/templates/ventas/perfil_cliente.html'

with open(path, 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Quitar 'table-responsive' de todos lados para que nada colapse o esconda botones,
# pero en vez de borrar el div, sólo le quito la clase 'table-responsive' y le pongo
# algo inofensivo como 'w-100' para que HTML no se rompa por falta del </div>.
html = html.replace('<div class="table-responsive">', '<div class="w-100 overflow-visible">')

# 2. Quitar el Email de Contactos
html = html.replace('<th>Email</th>', '')
html = html.replace('<td>{{ c.email }}</td>', '')
html = html.replace('<td colspan="5" class="text-center text-muted py-3">No hay contactos', 
                    '<td colspan="4" class="text-center text-muted py-3">No hay contactos')

# 3. Agregar "Cuenta Corriente" justo antes del {% endblock %}
# Validamos de no duplicar si ya existe:
if 'CUENTA CORRIENTE' not in html:
    new_section = '''
<!-- CUENTA CORRIENTE -->
<h5 class="mt-4 mb-2">
    Cuenta Corriente
    {% if cuenta_corriente %}
    &mdash; Saldo:
    <strong class="{% if cuenta_corriente[-1].saldo > 0 %}text-danger{% elif cuenta_corriente[-1].saldo < 0 %}text-success{% else %}text-muted{% endif %}">
        ${{ "%.2f"|format(cuenta_corriente[-1].saldo) }}
    </strong>
    {% endif %}
</h5>

{% if not cuenta_corriente %}
<p class="text-muted">Sin movimientos registrados.</p>
{% else %}
<table class="table table-sm table-hover">
    <thead class="table-light">
        <tr>
            <th>Fecha</th>
            <th>Tipo</th>
            <th>Nro. Documento</th>
            <th>Nro. Recibo</th>
            <th>Doc. Aplicado</th>
            <th class="text-end">Debe</th>
            <th class="text-end">Haber</th>
            <th class="text-end">Saldo</th>
            <th class="text-end" title="Percepciones incluidas en el importe (informativo)">Percep.</th>
            <th class="text-end" title="Retenciones sufridas (informativo)">Ret.</th>
            <th class="text-center">Asiento</th>
        </tr>
    </thead>
    <tbody>
        {% for mov in cuenta_corriente %}
        <tr>
            <td>{{ mov.fecha.strftime('%d/%m/%Y') if mov.fecha else '-' }}</td>
            <td>
                {% if mov.tipo_doc == 'REC' %}RECIBO
                {% elif mov.tipo_doc in ['003','008','013'] %}N/CRÉD
                {% elif mov.tipo_doc in ['005','010','015'] %}N/DÉB
                {% else %}FACTURA{% endif %}
            </td>
            <td>
                {% if mov.nro_documento and mov.comprobante_id %}
                    <a href="{{ url_for('ventas.ver_comprobante', id=mov.comprobante_id) }}">{{ mov.nro_documento }}</a>
                {% else %}
                    {{ mov.nro_documento or '-' }}
                {% endif %}
            </td>
            <td>{{ mov.nro_recibo or '-' }}</td>
            <td>{{ mov.nro_doc_aplicado or '-' }}</td>
            <td class="text-end">{% if mov.debe > 0 %}${{ "%.2f"|format(mov.debe) }}{% else %}-{% endif %}</td>
            <td class="text-end">{% if mov.haber > 0 %}${{ "%.2f"|format(mov.haber) }}{% else %}-{% endif %}</td>
            <td class="text-end fw-bold {% if mov.saldo > 0.005 %}text-danger{% elif mov.saldo < -0.005 %}text-success{% endif %}">
                ${{ "%.2f"|format(mov.saldo) }}
            </td>
            <td class="text-end text-muted small">
                {% if mov.total_percepciones > 0 %}${{ "%.2f"|format(mov.total_percepciones) }}{% else %}-{% endif %}
            </td>
            <td class="text-end text-muted small">
                {% if mov.total_retenciones > 0 %}${{ "%.2f"|format(mov.total_retenciones) }}{% else %}-{% endif %}
            </td>
            <td class="text-center">
                {% if mov.asiento_id %}
                <a href="{{ url_for('contabilidad.ver_asiento', id=mov.asiento_id) }}"
                   class="btn btn-sm btn-outline-secondary py-0 px-1" title="Ver Asiento #{{ mov.asiento_id }}">
                    <i class="fas fa-book"></i>
                </a>
                {% else %}
                <span class="text-muted small">-</span>
                {% endif %}
            </td>
        </tr>
        {% endfor %}
    </tbody>
    <tfoot>
        <tr class="fw-bold table-light">
            <td colspan="5">TOTAL</td>
            <td class="text-end">${{ "%.2f"|format(cuenta_corriente | sum(attribute='debe')) }}</td>
            <td class="text-end">${{ "%.2f"|format(cuenta_corriente | sum(attribute='haber')) }}</td>
            <td class="text-end {% if cuenta_corriente[-1].saldo > 0.005 %}text-danger{% elif cuenta_corriente[-1].saldo < -0.005 %}text-success{% endif %}">
                ${{ "%.2f"|format(cuenta_corriente[-1].saldo) }}
            </td>
            <td class="text-end text-muted">${{ "%.2f"|format(cuenta_corriente | sum(attribute='total_percepciones')) }}</td>
            <td class="text-end text-muted">${{ "%.2f"|format(cuenta_corriente | sum(attribute='total_retenciones')) }}</td>
            <td></td>
        </tr>
    </tfoot>
</table>
{% endif %}
'''
    html = html.replace('{% endblock %}', new_section + '\n\n{% endblock %}')

with open(path, 'w', encoding='utf-8') as f:
    f.write(html)

print("Restaurado todo!")
