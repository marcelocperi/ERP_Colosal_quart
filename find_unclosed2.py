path = 'multiMCP/core/templates/admin_risk_dashboard.html'

with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

import re

stack = []
for i, line in enumerate(lines, 1):
    opens = re.findall(r'\{%-?\s*(if|for|block)\b', line)
    closes = re.findall(r'\{%-?\s*(endif|endfor|endblock)\b', line)
    for tag in opens:
        stack.append((i, tag))
    for c in closes:
        matched = {'endif': 'if', 'endfor': 'for', 'endblock': 'block'}.get(c)
        if stack and stack[-1][1] == matched:
            stack.pop()
        elif stack:
            print(f"Line {i}: '{c}' closed '{stack[-1][1]}' (mismatch!)")
            stack.pop()
        else:
            print(f"Line {i}: Unexpected '{c}' (nothing open)")

print("\nUnclosed at end:")
for lineno, tag in stack:
    print(f"  Line {lineno}: '{tag}' never closed")
