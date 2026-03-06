import sys

path = 'ventas/templates/ventas/clientes.html'
with open(path, 'r', encoding='utf-8') as f:
    html = f.read()

# Remove table responsive
html = html.replace('<div class="table-responsive table-fixed-header">', '<div class="table-fixed-header overflow-visible">')

# Modify Email column in header
html = html.replace('<th>Email</th>', '')

# Remove filter column for email
filter_email = '''<th><select class="filter-select" data-col="5">
                                    <option value="">Todos</option>
                                </select></th>'''
html = html.replace(filter_email, '')

# Remove email data cell
html = html.replace('<td class="small">{{ c.email or \'-\' }}</td>', '')

with open(path, 'w', encoding='utf-8') as f:
    f.write(html)

print("Modificado clientes.html")
