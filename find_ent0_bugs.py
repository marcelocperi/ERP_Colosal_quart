import os
import re

def search_potential_issues():
    patterns = [
        r'if\s+([a-zA_Z0-9_.]*enterprise_id):',
        r'if\s+not\s+([a-zA_Z0-9_.]*enterprise_id):',
        r'if\s+([a-zA_Z0-9_.]*ent_id):',
        r'if\s+not\s+([a-zA_Z0-9_.]*ent_id):',
    ]
    
    for root, dirs, files in os.walk('.'):
        if 'venv' in root or '.git' in root:
            continue
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    for pattern in patterns:
                        for match in re.finditer(pattern, content):
                            print(f"ISSUE in {path}: {match.group(0)}")

if __name__ == "__main__":
    search_potential_issues()
