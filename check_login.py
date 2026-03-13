import requests
import sys

session = requests.Session()
url = "http://127.0.0.1/login"

r = session.get(url)
print(f"GET /login: {r.status_code}")

# Get CSRF if any (it's exempt but just in case)
data = {
    'enterprise_id': '0',
    'username': 'superadmin',
    'password': 'super'
}

r2 = session.post(url, data=data, allow_redirects=False)
print(f"POST /login: {r2.status_code}")
print(f"Headers: {r2.headers}")
if r2.status_code == 302:
    redirect_url = r2.headers.get('Location')
    print(f"Redirects to: {redirect_url}")
    
    # Follow redirect manually
    r3 = session.get("http://127.0.0.1" + redirect_url if redirect_url.startswith("/") else redirect_url, allow_redirects=False)
    print(f"GET Dashboard: {r3.status_code}")
    print(f"Dashboard Headers: {r3.headers}")
    
    if r3.status_code == 302:
        print(f"Redirects AGAIN to: {r3.headers.get('Location')}")
    else:
        print(r3.text[:500])
