# dataMasterGK - Middleware de Integración para GK

![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg) ![Framework](https://img.shields.io/badge/Framework-Flask-red.svg)

**dataMasterGK** es una solución ETL (Extract, Transform, Load) desarrollada en Python y Flask, diseñada para automatizar la integración de datos maestros con sistemas GK Software. La aplicación monitorea directorios locales, procesa archivos Excel (Tiendas, Operadores, Promociones), los transforma a formato XML estándar de GK y los transmite a un servidor central vía SFTP o FTP.

## ✨ Características

- **Panel de Control Web:** Interfaz intuitiva para monitorear, configurar y operar el sistema.
- **Procesamiento Automatizado:** Scheduler integrado para ejecuciones programadas por hora.
- **Ejecución Manual:** Dispara la ejecución de cualquier interfaz con un solo clic.
- **Interfaces Soportadas:**
    - Tiendas (`Store`)
    - Operadores (`Operator`)
    - Promociones (`Promotion`)
    - Promociones por Categoría (`Promotion Category`)
- **Logging Robusto:** Logs en tiempo real en la UI, en archivos locales y en una base de datos SQLite para auditoría.
- **Gestión de Archivos:** Carga de archivos vía "drag-and-drop" y archivado automático de archivos procesados (con éxito o con error).
- **Configuración Flexible:** Todas las rutas y credenciales del servidor se gestionan desde un único panel.
- **Transporte Seguro:** Soporte para SFTP y FTP.

## 🛠️ Stack Tecnológico

- **Backend:** Python 3, Flask
- **Base de Datos:** SQLite
- **ETL:** Pandas
- **Scheduler:** Schedule
- **Transporte:** Paramiko (SFTP), ftplib (FTP)
- **Frontend:** HTML, TailwindCSS, JavaScript

## 🚀 Instalación y Puesta en Marcha

Sigue estos pasos para configurar y ejecutar el proyecto en tu entorno local.

### 1. Prerrequisitos

- Python 3.9 o superior.
- `pip` y `venv`.

### 2. Clonar el Repositorio

```bash
git clone <URL-DEL-REPOSITORIO>
cd dataMasterGK
```

### 3. Crear Entorno Virtual e Instalar Dependencias

Es una buena práctica aislar las dependencias del proyecto en un entorno virtual.

```bash
# Crear entorno virtual
python -m venv venv

# Activar el entorno
# En Windows:
venv\Scripts\activate
# En macOS/Linux:
source venv/bin/activate

# Instalar las dependencias
pip install -r requirements.txt
```

### 4. Configuración Inicial

Antes de ejecutar la aplicación, es necesario configurar las rutas de los directorios y las credenciales del servidor.

1.  Ejecuta la aplicación por primera vez para generar el `config.json` por defecto.
2.  Abre la aplicación en tu navegador (`http://127.0.0.1:5000`).
3.  Ve a la sección **Configuración**.
4.  Rellena las rutas absolutas de los directorios donde se depositarán los archivos Excel para cada interfaz.
5.  Completa los datos del servidor SFTP/FTP (host, usuario, contraseña, etc.).
6.  Guarda la configuración.

### 5. Inicializar la Base de Datos

El sistema utiliza una base de datos SQLite para guardar logs, historial y la configuración del scheduler.

```bash
python init_db.py
```
> La base de datos también se crea automáticamente al iniciar el servidor `app.py` si no existe.

### 6. Ejecutar la Aplicación

Una vez configurado, inicia el servidor Flask.

```bash
python app.py
```

La aplicación estará disponible en `http://127.0.0.1:5000`. El scheduler comenzará a funcionar en segundo plano.