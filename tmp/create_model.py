
import requests
import os

def create_colosal_model():
    print("Iniciando creación de modelo colosal-auditor...")
    url = "http://localhost:11434/api/create"
    
    # Leer el Modelfile
    with open("Modelfile", "r", encoding="utf-8") as f:
        modelfile_content = f.read()
    
    # IMPORTANTE: El campo debe ser 'modelfile' (en minúsculas)
    payload = {
        "name": "colosal-auditor",
        "modelfile": modelfile_content, 
        "stream": False
    }
    
    try:
        # Aumentamos el timeout porque la creación puede tardar si tiene que procesar
        response = requests.post(url, json=payload, timeout=120) 
        if response.status_code == 200:
            print("✓ Modelo 'colosal-auditor' creado exitosamente.")
            print(response.json())
        else:
            print(f"Error: Status {response.status_code}")
            print(f"Detalle: {response.text}")
    except Exception as e:
        print(f"Error conectando con Ollama: {e}")

if __name__ == "__main__":
    create_colosal_model()
