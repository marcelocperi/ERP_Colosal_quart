# db_cli_mandatory.py
"""Utility to enforce that all DML/DDL operations are performed via db_cli.

Usage:
    python tmp/db_cli_mandatory.py "SQL STATEMENT"

The script will refuse to run any SELECT queries and will only allow
INSERT, UPDATE, DELETE, ALTER, CREATE, DROP, TRUNCATE, etc.
If a non‑DML/DDL statement is passed, an exception is raised.
"""
import sys
import re
import subprocess
from pathlib import Path

DB_CLI_PATH = Path(__file__).parent / "db_cli.py"

DML_DDL_REGEX = re.compile(r"^(\s*)(INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE|REPLACE|GRANT|REVOKE)", re.IGNORECASE)

def main():
    if len(sys.argv) < 2:
        print("Usage: python db_cli_mandatory.py \"SQL STATEMENT\"")
        sys.exit(1)
    sql = sys.argv[1]
    if not DML_DDL_REGEX.match(sql):
        raise ValueError("Only DML/DDL statements are allowed through db_cli_mandatory. Received: " + sql)
    subprocess.run([sys.executable, str(DB_CLI_PATH), sql], check=True)

if __name__ == "__main__":
    main()
