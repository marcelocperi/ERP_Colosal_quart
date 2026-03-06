# 🐍 Hosting Gratuito en PythonAnywhere (Guía Dummies)

Si buscas una opción **100% GRATIS** para siempre (con algunas limitaciones) para hospedar tu aplicación Flask + MySQL, **PythonAnywhere** es la mejor opción.

> **Limitaciones del Plan Gratuito:**
> *   Tu sitio será `tu_usuario.pythonanywhere.com`.
> *   Debes hacer clic en un botón "Run until 3 months from now" cada 3 meses en su panel para que no borren la app.
> *   Solo puedes conectar a la base de datos MySQL que ellos te dan (no externas).
> *   El envío de emails (SMTP) suele estar restringido a ciertos proveedores.

---

## 📝 Paso 1: Registro

1.  **Haz clic aquí para crear tu cuenta gratuita**: [Registrarse en PythonAnywhere (Plan Beginner)](https://www.pythonanywhere.com/registration/register/beginner/)
2.  Completa el formulario (Usuario, Email, Password).
3.  Acepta los términos y haz clic en **Register**.
4.  ¡Listo! Ya tienes tu servidor gratuito.

---

## 📤 Paso 2: Subir tu Código

1.  En tu computadora, comprime toda la carpeta de tu proyecto (`bibliotecaweb`) en un archivo **ZIP**.
2.  En PythonAnywhere, ve a la pestaña **Files**.
3.  Sube tu archivo ZIP.
4.  Abre una **Bash Console** (desde el Dashboard o pestaña Consoles).
5.  Escribe: `unzip nombre_de_tu_archivo.zip` (reemplaza por el nombre real).
6.  (Opcional pero recomendado) Crea un entorno virtual:
    ```bash
    mkvirtualenv --python=/usr/bin/python3.10 mi-entorno
    ```
7.  Instala las librerías.
    > **⚠️ ATENCIÓN:** La librería `mariadb` a veces da problemas para instalarse en el plan gratuito porque requiere compilación. Si te falla, te recomiendo cambiar `mariadb` por `pymysql` (que es idéntico pero más compatible).
    
    Intenta primero:
        ```bash
        pip install -r requirements.txt
        ```
    Si falla con errores rojos de C/C++, edita tu `requirements.txt` y cambia `mariadb` por `pymysql`, y luego en tu archivo `database.py` cambia `import mariadb` por `import pymysql as mariadb` (truco rápido).

---

## 🗄️ Paso 3: Configurar la Base de Datos (Modo Gratuito: SQLite)

**¡Atención!** PythonAnywhere ya no ofrece MySQL en cuentas gratuitas nuevas (o es limitado).
Para que tu aplicación funcione **GRATIS**, usaremos **SQLite** (una base de datos que vive en un archivo). 
No te preocupes, he preparado todo para que sea automático.

1.  Abre una **Bash Console**.
2.  Ejecuta este comando para descargar las dependencias y configurar la base de datos:
    ```
    
    python3 setup_stock_sqlite.py
    ```
    *(Estos scripts crearán `multi_mcp.db` con usuarios, empresas, y el nuevo módulo de STOCK PRO).*

---

## ⚙️ Paso 4: Configurar la Aplicación Web

1.  Ve a la pestaña **Web**.
2.  Haz clic en **Add a new web app**.
3.  Selecciona **Manual Configuration** (Python 3.10).
4.  **Virtualenv**: (Opcional, si usaste uno).
5.  **Code Security (WSGI configuration file)**:
    *   Edita el archivo WSGI y pon esto:

    ```python
    import sys
    import os

    # Ruta a tu carpeta
    path = '/home/TU_USUARIO/multiMCP'  
    if path not in sys.path:
        sys.path.append(path)

    # Configuración para usar SQLite (¡IMPORTANTE!)
    os.environ['DB_TYPE'] = 'sqlite'
    os.environ['DB_NAME'] = 'multi_mcp.db'
    os.environ['SECRET_KEY'] = 'una_clave_secreta_dificil'

    from app import app as application
    ```

6.  Guardar.

---

## ✅ Paso 5: ¡Probar!

1.  Haz clic en el botón **Reload** (verde).
2.  Visita tu sitio: `https://tu_usuario.pythonanywhere.com`.
3.  Ingresa con:
    *   **Usuario**: `admin`
    *   **Contraseña**: `admin123`
    *   **Empresa**: `Carabobo`

Si algo falla, revisa el archivo **Error Log** (enlace en la pestaña Web).

---

## 🛠️ Tips Adicionales

### ¿Cómo ver los datos?

### ¿Cómo ver los datos?
Tu base de datos es el archivo `multi_mcp.db`. Puedes descargarla desde la pestaña **Files** para abrirla en tu PC.
