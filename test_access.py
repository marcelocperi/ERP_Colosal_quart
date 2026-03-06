
import requests

# Assuming local dev server is running on 5005
BASE_URL = "http://127.0.0.1:5005"

def test_usuarios_access():
    # We need to simulate a login first to get the session cookie
    s = requests.Session()
    
    # 1. Login
    print("Logging in...")
    login_data = {
        'username': 'admin',
        'password': '123' # Assuming standard dev password, if not we might fail here
    }
    r = s.post(f"{BASE_URL}/login", data=login_data, allow_redirects=True)
    print(f"Login Response: {r.status_code}")
    if "Dashboard" not in r.text and "INICIO" not in r.text:
       print("Login might have failed or redirected unexpectedly.")
       # check where we are
       print(f"URL: {r.url}")
    
    # Get the sid from the url or cookie? 
    # In this app, sid is usually passed as query param or stored in session?
    # Based on app.py, `g.sid` comes from args or form. 
    # But usually login redirects to /dashboard?sid=...
    
    import urllib.parse
    parsed = urllib.parse.urlparse(r.url)
    qs = urllib.parse.parse_qs(parsed.query)
    sid = qs.get('sid', [''])[0]
    
    print(f"SID obtained: {sid}")
    
    if not sid:
        print("No SID found, cannot proceed.")
        return

    # 2. Access /usuarios
    print(f"Accessing /usuarios?sid={sid} ...")
    r_u = s.get(f"{BASE_URL}/usuarios", params={'sid': sid}, allow_redirects=False)
    print(f"Usuarios Response: {r_u.status_code}")
    print(f"Location: {r_u.headers.get('Location')}")
    
    if r_u.status_code == 302:
        print("Redirected! Likely to login.")
    elif r_u.status_code == 200:
        print("Success! Page loaded.")
    else:
        print("Error or other status.")

if __name__ == "__main__":
    test_usuarios_access()
