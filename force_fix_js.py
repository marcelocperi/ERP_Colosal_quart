import os

path = 'C:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP/core/templates/admin_risk_dashboard.html'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the { { and } } mess
# Also fix any other weird spacing in variables
content = content.replace('{ { fm.total or 0 } }', '{{ fm.total or 0 }}')
content = content.replace('{ { t.total or 0 } }', '{{ t.total or 0 }}')
content = content.replace('{ { t.errors or 0 } }', '{{ t.errors or 0 }}')
content = content.replace('{ { fm.total } }', '{{ fm.total or 0 }}')
content = content.replace('{ { t.total } }', '{{ t.total or 0 }}')
content = content.replace('{ { t.errors } }', '{{ t.errors or 0 }}')

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Forced JS variables fix successfully.")
