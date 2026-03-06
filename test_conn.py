
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()


def test(host):
    print(f"--- Testing {host} ---")
    try:
        conn = pymysql.connect(
            host=host,
            port=int(os.environ.get("DB_PORT", 3307)),
            user=os.environ.get("DB_USER", "root"),
            password=os.environ.get("DB_PASSWORD", ""),
            database=os.environ.get("DB_NAME", "multi_mcp_db")
        )
        print(f"✅ SUCCESS: Connected to {host}")
        conn.close()
    except Exception as e:
        print(f"❌ FAIL: {host}: {type(e).__name__}: {e}")

if __name__ == "__main__":
    test("localhost")
    test("127.0.0.1")
    print("--- Done ---")

