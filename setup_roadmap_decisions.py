import os
import json
from database import get_db_cursor
from datetime import datetime

def setup_roadmap_table():
    print("Configurando tabla de Registro de Decisiones y Hoja de Ruta...")
    
    with get_db_cursor() as cursor:
        # 1. Crear la tabla
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_roadmap_decisions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT DEFAULT 0,
                modulo VARCHAR(100),
                subcategoria VARCHAR(100),
                funcionalidad VARCHAR(255),
                descripcion_ampliada TEXT,
                decision VARCHAR(50),
                dt_user_created INT,
                dt_date_created DATETIME DEFAULT CURRENT_TIMESTAMP,
                dt_user_updated INT,
                dt_date_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX (enterprise_id),
                INDEX (modulo)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        # 2. Datos para poblar (Basados en nuestra conversación)
        # Nota: Usamos enterprise_id 0 como global/demo y user_id 1 como administrador
        data = [
            (0, 'Compras', 'Workflow', 'Segregación de Funciones (SoD)', 'Flujo lineal controlado donde el creador no puede aprobar para prevenir fraudes.', 'Completado', 1),
            (0, 'Compras', 'Seguridad', 'Excel Blindado Criptográfico', 'Plantillas de cotización con Password dinámico y Hash de integridad.', 'Completado', 1),
            (0, 'Stock', 'Hardware', 'Librería de Protocolos de Balanzas', 'Conectividad base para balanzas industriales líderes (Serial/IP/USB).', 'Completado', 1),
            (0, 'Stock', 'Hardware', 'Lógica Avanzada de Celdas de Carga', 'Gestión de pesaje complejo para múltiples balanzas concurrentes.', 'Cerrado', 1),
            (0, 'Stock', 'Catalogación', 'Identificación Multi-Código (Alias)', 'Soporte para GTIN, EAN13, ITF14 y códigos de proveedor en un mismo SKU.', 'Completado', 1),
            (0, 'Stock', 'Etiquetado', 'Motor de Impresión Directa (ZPL)', 'Impresión térmica silenciosa vía comandos RAW/ZPL directo a Zebra.', 'Completado', 1),
            (0, 'Stock', 'Trazabilidad', 'Gestión de Números de Serie', 'Trazabilidad unitaria con carga masiva de CSV de fabricantes.', 'Completado', 1),
            (0, 'Seguridad', 'Legal', 'Ficha Técnica GHS/SGA', 'Inclusión de pictogramas, UN y frases H/P en la ficha del producto.', 'Completado', 1),
            (0, 'Seguridad', 'Etiquetado', 'Etiquetas con Advertencia Visual', 'Logos de seguridad (Inflamables, etc) impresos directamente en la etiqueta.', 'Completado', 1),
            (0, 'Seguridad', 'Lógica', 'Motor de Incompatibilidad Química', 'Validación en tiempo real para impedir estibaje de químicos peligrosos opuestos.', 'Completado', 1),
            (0, 'Seguridad', 'Visualización', 'Mapa de Riesgo Coroplético', 'Visualización de racks con colores según carga de fuego acumulada.', 'Cerrado', 1),
            (0, 'Seguridad', 'Documental', 'MSDS/FDS Cloud + QR', 'Repositorio de fichas en PDF accesibles por QR desde la etiqueta.', 'Pendiente', 1),
            (0, 'Stock', 'UoM', 'Conversión Automática Master Box', 'Ingreso de stock multiplicando por factor al escanear ITF-14.', 'Pendiente', 1)
        ]
        
        # 3. Insertar solo si está vacío para evitar duplicados en re-ejecución
        cursor.execute("SELECT COUNT(*) as total FROM sys_roadmap_decisions")
        if cursor.fetchone()[0] == 0:
            cursor.executemany("""
                INSERT INTO sys_roadmap_decisions 
                (enterprise_id, modulo, subcategoria, funcionalidad, descripcion_ampliada, decision, dt_user_created, dt_user_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, [(d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[6]) for d in data])
            print(f"✅ Se insertaron {len(data)} registros de historial.")
        else:
            print("ℹ️ La tabla ya contiene datos. No se realizó la inserción masiva.")

if __name__ == "__main__":
    setup_roadmap_table()
