# -*- coding: utf-8 -*-
"""
Setup Matriz de Aprobaciones Dinámica (Fase 1)
==============================================
Cubre el Gap de Aprobación Multiescala y Escalamiento Customizable.
"""
import os
import sys

# Asegurar que el path incluya la carpeta actual para importar database
sys.path.insert(0, os.path.dirname(__file__))
from database import get_db_cursor

def create_workflow_tables():
    print("--- Iniciando Creación de Tablas de Workflow Dinámico ---")
    
    with get_db_cursor() as c:
        # 1. Tabla de REGLAS DE FLUJO
        # Define cuándo se dispara un flujo específico (ej: Compras > 50.000)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sys_workflow_rules (
                id INT PRIMARY KEY AUTO_INCREMENT,
                enterprise_id INT NOT NULL,
                module VARCHAR(50) NOT NULL DEFAULT 'COMPRAS',
                name VARCHAR(100) NOT NULL,
                condition_type ENUM('AMOUNT_GTE', 'ALWAYS', 'CATEGORY') DEFAULT 'AMOUNT_GTE',
                condition_value DECIMAL(18,2) DEFAULT 0.00,
                priority INT DEFAULT 1, -- Orden de evaluación de reglas
                is_active TINYINT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_ent_mod_prio (enterprise_id, module, priority)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        print("   [OK] sys_workflow_rules creada.")

        # 2. Tabla de PASOS DEL FLUJO
        # Define la escalera de aprobación (Nivel 1 -> Nivel 2 -> Nivel 3)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sys_workflow_steps (
                id INT PRIMARY KEY AUTO_INCREMENT,
                enterprise_id INT NOT NULL,
                rule_id INT NOT NULL,
                step_order INT NOT NULL, -- 1, 2, 3...
                role_id INT, -- Rol que debe aprobar
                user_id INT, -- Opcional: Usuario específico que debe aprobar
                description VARCHAR(255),
                min_approvals INT DEFAULT 1, -- Cuántas firmas se requieren en este paso
                FOREIGN KEY (rule_id) REFERENCES sys_workflow_rules(id) ON DELETE CASCADE,
                INDEX idx_rule_step (rule_id, step_order)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        print("   [OK] sys_workflow_steps creada.")

        # 3. Tabla de SEGUIMIENTO DE TRANSACCIONES (Instancias)
        # Sigue el progreso de una PO, Solicitud, etc.
        c.execute("""
            CREATE TABLE IF NOT EXISTS sys_transaction_approvals (
                id INT PRIMARY KEY AUTO_INCREMENT,
                enterprise_id INT NOT NULL,
                transaction_type VARCHAR(50) NOT NULL, -- 'CMP_PO', 'CMP_NP', etc.
                transaction_id INT NOT NULL,
                rule_id INT NOT NULL,
                current_step INT DEFAULT 1,
                status ENUM('PENDING', 'APPROVED', 'REJECTED') DEFAULT 'PENDING',
                history_json JSON, -- Log detallado de firmas
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_trans (enterprise_id, transaction_type, transaction_id),
                FOREIGN KEY (rule_id) REFERENCES sys_workflow_rules(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        print("   [OK] sys_transaction_approvals creada.")

        # 4. Tabla de FIRMAS INDIVIDUALES
        # Registro detallado de cada "click" de aprobación
        c.execute("""
            CREATE TABLE IF NOT EXISTS sys_approval_signatures (
                id INT PRIMARY KEY AUTO_INCREMENT,
                enterprise_id INT NOT NULL,
                approval_id INT NOT NULL,
                step_order INT NOT NULL,
                user_id INT NOT NULL,
                action ENUM('APPROVE', 'REJECT') NOT NULL,
                comment TEXT,
                signature_hash VARCHAR(128), -- Para cumplimiento CISA
                signed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (approval_id) REFERENCES sys_transaction_approvals(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES sys_users(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        print("   [OK] sys_approval_signatures creada.")

def seed_default_workflow_for_enterprise_zero():
    print("\n--- Seteando Workflows de Talla Mediana (Standards de Auditoría) ---")
    with get_db_cursor(dictionary=True) as c:
        # Purgar datos previos para enterprise 0
        c.execute("DELETE FROM sys_workflow_steps WHERE enterprise_id=0")
        c.execute("DELETE FROM sys_workflow_rules WHERE enterprise_id=0")
        
        # Obtener IDs de roles dinámicamente
        roles_needed = ['APROBADOR_COMPRAS', 'TESORERO', 'Admin']
        role_map = {}
        for r_name in roles_needed:
            c.execute("SELECT id FROM sys_roles WHERE name = %s AND enterprise_id=0", (r_name,))
            row = c.fetchone()
            if row: role_map[r_name] = row['id']

        # ---------------------------------------------------------------------
        # REGLA 1: COMPRAS PEQUEÑAS (< 5.000)
        # ---------------------------------------------------------------------
        c.execute("""
            INSERT INTO sys_workflow_rules (enterprise_id, module, name, condition_type, condition_value, priority)
            VALUES (0, 'COMPRAS', 'Compras Menores', 'ALWAYS', 0.00, 100)
        """)
        rule_small = c.lastrowid
        if 'APROBADOR_COMPRAS' in role_map:
            c.execute("""
                INSERT INTO sys_workflow_steps (enterprise_id, rule_id, step_order, role_id, description)
                VALUES (0, %s, 1, %s, 'Aprobación Jefe de Compras')
            """, (rule_small, role_map['APROBADOR_COMPRAS']))

        # ---------------------------------------------------------------------
        # REGLA 2: COMPRAS ESTÁNDAR (5.000 - 50.000)
        # ---------------------------------------------------------------------
        c.execute("""
            INSERT INTO sys_workflow_rules (enterprise_id, module, name, condition_type, condition_value, priority)
            VALUES (0, 'COMPRAS', 'Operaciones Estándar', 'AMOUNT_GTE', 5000.00, 50)
        """)
        rule_std = c.lastrowid
        if 'APROBADOR_COMPRAS' in role_map and 'TESORERO' in role_map:
            c.execute("""
                INSERT INTO sys_workflow_steps (enterprise_id, rule_id, step_order, role_id, description)
                VALUES (0, %s, 1, %s, 'Validación Compras')
            """, (rule_std, role_map['APROBADOR_COMPRAS']))
            c.execute("""
                INSERT INTO sys_workflow_steps (enterprise_id, rule_id, step_order, role_id, description)
                VALUES (0, %s, 2, %s, 'Autorización Financiera (Tesoreria)')
            """, (rule_std, role_map['TESORERO']))

        # ---------------------------------------------------------------------
        # REGLA 3: GRANDES INVERSIONES (> 50.000)
        # ---------------------------------------------------------------------
        c.execute("""
            INSERT INTO sys_workflow_rules (enterprise_id, module, name, condition_type, condition_value, priority)
            VALUES (0, 'COMPRAS', 'Grandes Inversiones / CAPEX', 'AMOUNT_GTE', 50000.00, 10)
        """)
        rule_large = c.lastrowid
        if all(k in role_map for k in ['APROBADOR_COMPRAS', 'TESORERO', 'Admin']):
            c.execute("""
                INSERT INTO sys_workflow_steps (enterprise_id, rule_id, step_order, role_id, description)
                VALUES (0, %s, 1, %s, 'Validación Técnica/Compras')
            """, (rule_large, role_map['APROBADOR_COMPRAS']))
            c.execute("""
                INSERT INTO sys_workflow_steps (enterprise_id, rule_id, step_order, role_id, description)
                VALUES (0, %s, 2, %s, 'Control Financiero')
            """, (rule_large, role_map['TESORERO']))
            c.execute("""
                INSERT INTO sys_workflow_steps (enterprise_id, rule_id, step_order, role_id, description)
                VALUES (0, %s, 3, %s, 'Aprobación Directorio / CEO')
            """, (rule_large, role_map['Admin']))
            
    print("   [OK] Workflows multicapa para Empresa Mediana inicializados.")
            
    print("   [OK] Workflow 'Estandar Compras' inicializado con 2 niveles.")

if __name__ == "__main__":
    try:
        create_workflow_tables()
        seed_default_workflow_for_enterprise_zero()
        print("\n=== SETUP WORKFLOW FINALIZADO CON ÉXITO ===")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
