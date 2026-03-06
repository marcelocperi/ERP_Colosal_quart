import os
from waitress import serve
from app import app
from dotenv import load_dotenv

load_dotenv()
port = int(os.environ.get('PORT', 80))

print(f"Testing binding to port {port}...")
try:
    # A tiny mock app just to test bind
    def hello(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"Hello World"]
    
    # We only want to test if it CAN bind, so we'll close it right away
    # Waitress doesn't have a simple 'test_bind', so we'll use a standard socket test first
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', port))
    if result == 0:
        print(f"FAILED: Port {port} is ALREADY IN USE by another process.")
        sock.close()
    else:
        print(f"SUCCESS: Port {port} is FREE.")
        sock.close()

except Exception as e:
    print(f"ERROR: {e}")
