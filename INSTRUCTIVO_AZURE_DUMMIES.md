# ☁️ Guía de Despliegue en Azure para Principiantes (Dummies)

Esta guía te llevará paso a paso para subir tu aplicación **Biblioweb (MultiMCP)** a la nube de Microsoft Azure. ¡No necesitas ser un experto!

---

## 📋 Prerrequisitos
1.  **Cuenta de Azure**: Si no tienes una, crea una cuenta gratuita en [azure.microsoft.com](https://azure.microsoft.com/es-es/free/).
2.  **Código en GitHub**: Asegúrate de que tu código actualizado esté en tu repositorio de GitHub.
3.  **Herramienta de Base de Datos**: Recomendamos **MySQL Workbench** o **DBeaver** instalado en tu PC para conectar a la base de datos de la nube.

---

## 🚀 Paso 1: Crear la Base de Datos (Azure Database for MariaDB/MySQL)

1.  En el [Portal de Azure](https://portal.azure.com), busca **"Azure Database for MySQL"** (o MariaDB).
2.  Haz clic en **Crear**.
3.  Selecciona **Servidor Flexible** (es más barato y moderno).
4.  Completa los datos:
    *   **Grupo de Recursos**: Crea uno nuevo (ej: `rg-biblioteca`).
    *   **Nombre del servidor**: Un nombre único (ej: `db-biblioweb-prod`).
    *   **Región**: Elige la más cercana (ej: `East US`).
    *   **Versión**: MySQL 5.7 o 8.0 (recomendado 8.0).
    *   **Autenticación**: Selecciona "Autenticación de MySQL".
    *   **Usuario admin**: ej: `adminuser`.
    *   **Contraseña**: Crea una contraseña segura y **ANÓTALA**.
5.  En **Redes (Networking)**:
    *   Marca la opción **"Permitir acceso público desde cualquier servicio de Azure..."** (Esto es vital para que la Web App se conecte).
    *   Agrega tu dirección IP actual (botón "Agregar dirección IP del cliente actual") para que tú puedas conectarte desde tu PC.
6.  Haz clic en **Revisar y crear** -> **Crear**. (Esto tardará unos 5-10 minutos).

---

## 🌐 Paso 2: Crear la Aplicación Web (Azure App Service)

1.  En el Portal, busca **"App Services"** (Servicios de aplicaciones).
2.  Haz clic en **Crear** -> **Web App**.
3.  Completa los datos:
    *   **Grupo de Recursos**: Usa el mismo de antes (`rg-biblioteca`).
    *   **Nombre**: Nombre único para tu web (ej: `biblioweb-app`). Será tu dirección: `biblioweb-app.azurewebsites.net`.
    *   **Pila del entorno (Runtime Stack)**: Selecciona **Python 3.10** (o la versión que uses, 3.11 también sirve).
    *   **Sistema Operativo**: **Linux**.
    *   **Plan de Precios**: Para pruebas, elige **B1** (Basic) o **F1** (Gratis, si está disponible, aunque B1 es más estable).
4.  Haz clic en **Revisar y crear** -> **Crear**.

---

## ⚙️ Paso 3: Configurar Variables de Entorno

Para que la App sepa cómo conectarse a la Base de Datos sin poner las claves en el código:

1.  Ve a tu recurso **App Service** recién creado.
2.  En el menú izquierdo, busca **Configuración** (o **Configuration**).
3.  En la pestaña **Configuración de la aplicación**, haz clic en **+ Nueva configuración de la aplicación**.
4.  Agrega las siguientes variables una por una:

| Nombre (Name) | Valor (Value) |
| :--- | :--- |
| `DB_HOST` | El *Nombre del servidor* que creaste en el Paso 1 (ej: `db-biblioweb-prod.mysql.database.azure.com`) |
| `DB_USER` | Tu usuario admin (del Paso 1) |
| `DB_PASSWORD` | Tu contraseña de la BD (del Paso 1) |
| `DB_NAME` | `multi_mcp_db` (o el nombre que quieras dar a tu BD) |
| `DB_PORT` | `3306` (Azure usa puerto 3306 por defecto, no 3307) |
| `FLASK_ENV` | `production` |
| `SCM_DO_BUILD_DURING_DEPLOYMENT` | `true` |

5.  Haz clic en **Guardar** (arriba).

---

## 🔗 Paso 4: Conectar GitHub y Desplegar

1.  En el menú izquierdo del App Service, busca **Centro de implementación (Deployment Center)**.
2.  En **Origen (Source)**, elige **GitHub**.
3.  Autoriza tu cuenta de GitHub si te lo pide.
4.  Selecciona:
    *   **Organización**: Tu usuario.
    *   **Repositorio**: `bibliotecaweb`.
    *   **Rama (Branch)**: `main` (o la rama donde esté tu código final).
5.  Haz clic en **Guardar**.
6.  Azure comenzará a descargar tu código e instalar los requisitos (`requirements.txt`). Puedes ver el progreso en la pestaña **Registros (Logs)**.

---

## 🗄️ Paso 5: Inicializar la Base de Datos

Tu aplicación está desplegada, pero la base de datos está vacía.

1.  Abre **MySQL Workbench** (o DBeaver) en tu PC.
2.  Crea una nueva conexión con los datos de Azure (Host, Usuario, Password, Puerto 3306).
3.  Conéctate.
4.  Crea la base de datos si no existe: `CREATE DATABASE multi_mcp_db;`
5.  Abre el archivo `erp_schema.sql` de tu proyecto.
6.  Ejecuta todo el script SQL para crear las tablas.
7.  **(Importante)**: No olvides insertar el usuario administrador inicial si el script no lo hace.
    ```sql
    INSERT INTO sys_enterprises (id, nombre, estado) VALUES (1, 'Mi Empresa', 'activo');
    INSERT INTO sys_roles (id, enterprise_id, name) VALUES (1, 1, 'AdminSys');
    -- Insertar usuario admin (Password: Admin123 hash es necesario, usa el script de seed si tienes uno)
    ```

---

## ✅ ¡Listo!

Visita tu URL (ej: `https://biblioweb-app.azurewebsites.net`).
Si ves un error de "Application Error", ve a **Herramientas de desarrollo -> Secuencia de registro (Log stream)** en Azure para ver qué pasó (usualmente falta alguna librería o la conexión a BD falló).

### Tip Extra: Comando de Inicio
Si Azure no detecta cómo iniciar tu app, ve a **Configuración -> Configuración general -> Comando de inicio** y escribe:
`gunicorn --bind=0.0.0.0 --timeout 600 app:app`
