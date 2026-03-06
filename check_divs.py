import sys
from bs4 import BeautifulSoup
from inspect import formatargvalues

path = 'ventas/templates/ventas/perfil_cliente.html'
with open(path, 'r', encoding='utf-8') as f:
    html = f.read()

# Instead of full parse, let's just do a simple stack trace of div tags
stack = []
for i, line in enumerate(html.split('\n'), 1):
    opens = line.count('<div')
    closes = line.count('</div')
    if opens > 0 or closes > 0:
        for _ in range(opens): stack.append(('div', i))
        for _ in range(closes):
            if stack: stack.pop()
            else: print(f"ERROR: Extra </div> at line {i}")

if stack:
    print(f"Unclosed divs left: {len(stack)}")
    for t, l in stack:
        print(f"Unclosed <div> opened at line {l}")
    
