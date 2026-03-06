import sys, json
sys.path.insert(0, r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')

from database import get_db_cursor

out = []
with get_db_cursor(dictionary=True) as cur:
    cur.execute(
        'SELECT id, error_message, clob_data FROM sys_transaction_logs WHERE id IN (651,655,656,657,658) ORDER BY id DESC'
    )
    rows = cur.fetchall()
    for r in rows:
        out.append(f"=== ID {r['id']} ===")
        out.append(f"Error: {r['error_message']}")
        clob = r.get('clob_data', '') or ''
        if clob:
            try:
                d = json.loads(clob)
                for k, v in d.items():
                    out.append(f"  {k}: {str(v)[:600]}")
            except Exception:
                out.append(str(clob)[:600])
        out.append('')

with open(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\tmp\incidents_out.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(out))

print('Done - wrote to tmp/incidents_out.txt')
