import os

path = 'C:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP/core/templates/admin_risk_dashboard.html'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Global Row/Margin Gaps
content = content.replace('row g-4 mb-4', 'row g-2 mb-3')
content = content.replace('row g-3 mb-4', 'row g-2 mb-3')

# 2. Card Body Padding
# This might be aggressive, but user wants "one shot"
content = content.replace('card-body py-4', 'card-body py-2')

# 3. List and Table Row Padding
content = content.replace('list-group-item bg-transparent border-secondary py-2 px-3', 'list-group-item bg-transparent border-secondary py-1 px-2')
content = content.replace('class="heatmap-row', 'class="heatmap-row extra-small')

# 4. Specific Container Heights (Manual Scroll)
# Heatmap
content = content.replace('style="height: 180px; overflow-y: auto;"', 'style="height: 150px; overflow-y: auto;"')
# Failure Modes
content = content.replace('canvas id="failureModeChart" height="200"', 'canvas id="failureModeChart" height="100"')
# Mitigations (If not already changed)
content = content.replace('height: 130px; overflow-y: auto;', 'height: 110px; overflow-y: auto;')
# Critical Events
content = content.replace('max-height: 150px; overflow-y: auto;', 'height: 130px; overflow-y: auto;')

# 5. Font Sizes
content = content.replace('class="text-white small fw-bold"', 'class="text-white extra-small fw-bold"')
content = content.replace('span class="badge bg-info-subtle text-info small"', 'span class="badge bg-info-subtle text-info extra-small"')

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Mass compaction performed.")
