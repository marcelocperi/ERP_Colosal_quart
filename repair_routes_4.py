import os

path = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\stock\routes.py'
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i in range(len(lines)):
    # Fix first block (UPDATE)
    if 'pictos = request.form.getlist(\'ghs_pictos\') # Lista de códigos GHSXX' in lines[i]:
        # Indent the following cursor.execute block
        j = i + 1
        while j < len(lines) and 'ON DUPLICATE KEY UPDATE' not in lines[j] and 'id_articulo, ent_id' not in lines[j]:
             lines[j] = '    ' + lines[j]
             j += 1
        # Also indent the closing part
        if j < len(lines):
             lines[j] = '    ' + lines[j]
             lines[j+1] = '    ' + lines[j+1]
             lines[j+2] = '    ' + lines[j+2]
             lines[j+3] = '    ' + lines[j+3]
             lines[j+4] = '    ' + lines[j+4]
             lines[j+5] = '    ' + lines[j+5]
             lines[j+6] = '    ' + lines[j+6]
        lines.insert(j+7, '                    else:\n')
        lines.insert(j+8, '                        flash("No tiene permisos para modificar datos de seguridad industrial.", "warning")\n')

# Actually, this logic is too complex to script manually like this.
# I'll just use a more direct search-replace for the whole blocks.
