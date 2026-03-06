import os

path = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\stock\routes.py'
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Index 161 (Line 162) is '                              safety_alerts = ...'
# Index 162 (Line 163) should be '                              if safety_alerts:'

lines[162] = '                              if safety_alerts:\n'
lines[163] = '                                  for alert in safety_alerts:\n'
lines[164] = '                                      flash(alert["message"], "danger" if alert["severity"] == "DANGER" else "warning")\n'
lines[165] = '                                      if alert["severity"] == "DANGER":\n'
lines[166] = '                                          if request.form.get("safety_bypass") == "on":\n'
lines[167] = '                                              if not ("safety_bypass" in g.permissions or "all" in g.permissions):\n'
lines[168] = '                                                  raise Exception(f"ACCESO DENEGADO (SoD): No tiene permiso \'safety_bypass\' para omitir el bloqueo de: {alert[\'message\']}")\n'
lines[169] = '                                          else:\n'
lines[170] = '                                              raise Exception(f"BLOQUEO DE SEGURIDAD INDUSTRIAL: {alert[\'message\']}")\n'

with open(path, 'w', encoding='utf-8') as f:
    f.writelines(lines)
print("Route repair complete.")
