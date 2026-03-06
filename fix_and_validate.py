path = 'multiMCP/core/templates/admin_risk_dashboard.html'

with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: split {% if %}disabled{% \n endif %} in date_start input
content = content.replace(
    "required {% if timeframe !='range' %}disabled{%\n                        endif %}>",
    "required {% if timeframe != 'range' %}disabled{% endif %}>"
)

# Fix 2: same for date_end input (may be slightly different whitespace)
content = content.replace(
    "required {% if timeframe !='range' %}disabled{%\r\n                        endif %}>",
    "required {% if timeframe != 'range' %}disabled{% endif %}>"
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

# Validate with Jinja2
from jinja2 import Environment, TemplateSyntaxError
env = Environment()
try:
    env.parse(content)
    print("✅ Template OK - sin errores de sintaxis Jinja2")
except TemplateSyntaxError as e:
    print(f"❌ ERROR linea {e.lineno}: {e.message}")
