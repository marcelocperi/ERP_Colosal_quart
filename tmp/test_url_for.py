from app import app
from flask import url_for

with app.test_request_context():
    try:
        url = url_for('core.error_log')
        print(f"URL: {url}")
    except Exception as e:
        print(f"Error: {e}")
