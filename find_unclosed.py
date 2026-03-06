path = 'multiMCP/core/templates/admin_risk_dashboard.html'

with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

depth = 0
block_stack = []
for i, line in enumerate(lines, 1):
    import re
    # Count opening blocks
    opens = re.findall(r'\{%-?\s*(if|for|block|macro|call|filter|with|set)\b', line)
    closes = re.findall(r'\{%-?\s*(endif|endfor|endblock|endmacro|endcall|endfilter|endwith)\b', line)
    for tag in opens:
        block_stack.append((i, tag, line.strip()[:80]))
    for tag in closes:
        if block_stack:
            block_stack.pop()
        else:
            print(f"Line {i}: Unexpected closing tag '{tag}'")

print("\nUnclosed blocks:")
for lineno, tag, content in block_stack:
    print(f"  Line {lineno}: opened '{tag}' -> {content}")
