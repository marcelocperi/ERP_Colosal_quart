import requests
import urllib3
urllib3.disable_warnings()

prefixes = ["fwshomo", "awshomo", "wshomo", "servicios1homo", "servicios1"]
suffixes = [
    "/oconws/services/CONService?wsdl",
    "/oconws/CONService?wsdl"
]

for p in prefixes:
    for s in suffixes:
        url = f"https://{p}.afip.gov.ar{s}"
        try:
            r = requests.get(url, verify=False, timeout=3)
            print(f"URL: {url} | Status: {r.status_code}")
            if r.status_code == 200 and ("wsdl" in r.text.lower() or "xml" in r.text.lower()):
                print(f"✅ FOUND WSDL at {url}")
                print(r.text[:500])
        except:
            pass
