import sys
sys.path.insert(0, '.')
from jinja2 import Environment, TemplateSyntaxError

for tpl_path in [
    'core/templates/admin_risk_dashboard.html',
    'ventas/templates/ventas/perfil_cliente.html',
]:
    try:
        src = open(tpl_path, encoding='utf-8').read()
        Environment().parse(src)
        print(f"✅ OK: {tpl_path}")
    except TemplateSyntaxError as e:
        print(f"❌ ERROR {tpl_path} linea {e.lineno}: {e.message}")
