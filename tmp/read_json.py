import json
import os

path = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\tmp\schema_full.json'
if os.path.exists(path):
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        data = f.read()
        print(data)
