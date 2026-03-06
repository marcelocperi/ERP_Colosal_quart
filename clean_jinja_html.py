import os

path = 'C:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP/core/templates/admin_risk_dashboard.html'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix broken Jinja tags with spaces
content = content.replace('{ {', '{{').replace('} }', '}}')

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Jinja tags cleaned.")
