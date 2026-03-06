import json
import csv
import os

def export_csv():
    json_path = 'publishers_to_process.json'
    csv_path = 'listado_editoriales.csv'
    
    if not os.path.exists(json_path):
        print(f"Error: {json_path} no existe. Ejecute primero 'list_publishers_to_enrich.py'.")
        return

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Editorial', 'Cantidad de Libros'])
        
        for item in data:
            writer.writerow([item['name'], item['count']])
            
    print(f"Listado exportado a: {os.path.abspath(csv_path)}")

if __name__ == "__main__":
    export_csv()
