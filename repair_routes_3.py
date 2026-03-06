import os

path = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\stock\routes.py'
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Localizar el bloque de seguridad industrial
start_idx = -1
for i, line in enumerate(lines):
    if 'safety_alerts = get_incompatibility_alerts' in line:
        start_idx = i
        break

if start_idx != -1:
    # Reemplazar el bloque (aprox 12 líneas)
    new_block = [
        '                              safety_alerts = get_incompatibility_alerts(incoming_s, existing_s)\n',
        '\n',
        '                              if safety_alerts:\n',
        '                                  for alert in safety_alerts:\n',
        '                                      flash(alert["message"], "danger" if alert["severity"] == "DANGER" else "warning")\n',
        '                                      if alert["severity"] == "DANGER":\n',
        '                                          if request.form.get("safety_bypass") == "on":\n',
        '                                              if not ("safety_bypass" in g.permissions or "all" in g.permissions):\n',
        '                                                  raise Exception(f"ACCESO DENEGADO (SoD): No tiene permiso \'safety_bypass\' para omitir el bloqueo de: {alert[\'message\']}")\n',
        '                                          else:\n',
        '                                              raise Exception(f"BLOQUEO DE SEGURIDAD INDUSTRIAL: {alert[\'message\']}")\n'
    ]
    
    # Encontrar el final del bloque (hasta 'Insertar Cabecera')
    end_idx = start_idx + 1
    for i in range(start_idx + 1, len(lines)):
        if '# Insertar Cabecera' in lines[i]:
            end_idx = i
            break
            
    # Reemplazar el rango
    lines[start_idx:end_idx] = new_block
    
    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print(f"Block replaced from line {start_idx+1} to {end_idx}")
else:
    print("Block not found!")
