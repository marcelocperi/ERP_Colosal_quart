import os

file_path = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\stock\routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

with open(file_path, 'w', encoding='utf-8') as f:
    for i, line in enumerate(lines):
        # Target lines 163-173 (0-indexed 162-172)
        if 162 <= i <= 172:
            # Strip whatever and re-indent to 30 spaces (same as 161)
            content = line.strip()
            if content:
                # Determine inner indentation
                indent_level = 0
                if content.startswith('for ') or content.startswith('if '):
                     # These are special cases. Actually, just handle the specific logic.
                     pass
                
                # Re-apply based on logic
                if 'if safety_alerts:' in line:
                    new_line = ' ' * 30 + 'if safety_alerts:\n'
                elif 'for alert in safety_alerts:' in line:
                    new_line = ' ' * 34 + 'for alert in safety_alerts:\n'
                elif "flash(alert['message']" in line:
                    new_line = ' ' * 38 + "flash(alert['message'], 'danger' if alert['severity'] == 'DANGER' else 'warning')\n"
                elif "if alert['severity'] == 'DANGER':" in line:
                    new_line = ' ' * 38 + "if alert['severity'] == 'DANGER':\n"
                elif "# Solo permitimos" in line:
                    new_line = ' ' * 42 + "# Solo permitimos omitir el bloqueo si hay autorización SoD (safety_bypass)\n"
                elif "if request.form.get('safety_bypass') == 'on':" in line:
                    new_line = ' ' * 42 + "if request.form.get('safety_bypass') == 'on':\n"
                elif "if not has_bypass_permission:" in line:
                    new_line = ' ' * 46 + "if not has_bypass_permission:\n"
                elif "raise Exception(f'ACCESO DENEGADO" in line:
                    new_line = ' ' * 50 + "raise Exception(f'ACCESO DENEGADO (SoD): No tiene permiso \"safety_bypass\" para omitir el bloqueo de: {alert[\"message\"]}')\n"
                elif "# Si tiene permiso" in line:
                    new_line = ' ' * 46 + "# Si tiene permiso, se deja pasar pero se registra como advertencia\n"
                elif "else:" in line and i > 165: # The else for safety_bypass
                    new_line = ' ' * 42 + "else:\n"
                elif "raise Exception(f'BLOQUEO DE" in line:
                    new_line = ' ' * 46 + "raise Exception(f'BLOQUEO DE SEGURIDAD INDUSTRIAL: {alert[\"message\"]}')\n"
                else:
                    new_line = ' ' * 30 + content + '\n'
                f.write(new_line)
            else:
                f.write('\n')
        else:
            f.write(line)
print("File repaired.")
