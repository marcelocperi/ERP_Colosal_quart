import os

path = 'C:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP/core/templates/admin_risk_dashboard.html'
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Fix lines 40-41 and 43-44 (compensating for 0-indexing)
# Line 40 is index 39
# Line 41 is index 40
# Line 43 is index 42
# Line 44 is index 43

new_lines = []
for i, line in enumerate(lines):
    if i == 39: # Line 40
        new_lines.append('                        name="date_start" value="{{ date_start or \'\' }}" required {% if timeframe != \'range\' %}disabled{% endif %}>\n')
    elif i == 40: # Line 41 (skip)
        continue
    elif i == 42: # Line 43
        new_lines.append('                        name="date_end" value="{{ date_end or \'\' }}" required {% if timeframe != \'range\' %}disabled{% endif %}>\n')
    elif i == 43: # Line 44 (skip)
        continue
    else:
        new_lines.append(line)

with open(path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("Rewrote lines 40-41 and 43-44 successfully.")
