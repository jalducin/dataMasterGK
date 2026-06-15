# dataMasterGK — Middleware ETL de datos maestros para GK Software

> Estado: en operación. Integra datos maestros (Tiendas, Operadores, Promociones y Promociones por
> Categoría) desde archivos Excel hacia sistemas GK Software vía SFTP/FTP.
> Flujo de trabajo: **Spec-Driven Development (OpenSpec)** — ver [Cómo contribuir](#-cómo-contribuir-sdd).

![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg) ![Framework](https://img.shields.io/badge/Framework-Flask-red.svg) ![DB](https://img.shields.io/badge/DB-SQLite-green.svg)

## 🎯 Qué resuelve

Automatiza la integración de datos maestros hacia GK, eliminando la carga manual de archivos al
servidor central:

- **Vigila** directorios locales por interfaz y procesa los Excel que se depositen.
- **Transforma** cada fila al XML estándar de GK (BusinessUnit, Operator, PromotionImport).
- **Transmite** los XML al servidor GK por SFTP o FTP.
- **Audita** todo (logs, XML generados, ejecuciones) en SQLite y archivos, con un panel web para
  configurar, ejecutar bajo demanda, programar por hora y consultar el historial.

Interfaces soportadas: `Store` (Tiendas), `Operator` (Operadores), `Promotion` (Promociones),
`Promotion Category` (Promociones por Categoría).

## 🏗️ Arquitectura

Monolito Flask con un pipeline ETL por interfaz:

```
┌──────────────────────────────────────────────────────────────────┐
│  PANEL WEB (navegador)                                             │
│  Jinja2 · TailwindCSS (CDN) · jQuery · multiple-select            │
└──────────────────────────────────────────────────────────────────┘
                    │  HTTP (Flask)
                    ▼
┌──────────────────────────────────────────────────────────────────┐
│  APLICACIÓN (Flask · app.py)                                       │
│  ┌────────────────┐ ┌──────────────────┐ ┌─────────────────────┐  │
│  │ Rutas panel    │ │ interface_runner  │ │ scheduler (hilo)    │  │
│  │ config/manual  │ │ ejecución manual  │ │ ejecución por hora  │  │
│  └────────────────┘ └──────────────────┘ └─────────────────────┘  │
├──────────────────────────────────────────────────────────────────┤
│  PIPELINE ETL — src/classes/{store,operator,promotion,…}.py        │
│  Excel (Pandas) → XML GK (ElementTree) → envío (utils) → archivado │
├──────────────────────────────────────────────────────────────────┤
│  INFRAESTRUCTURA — src/utils.py · src/log_database.py              │
│  Logging · SFTP/FTP (Paramiko/ftplib) · SQLite (auditoría)         │
└──────────────────────────────────────────────────────────────────┘
                    │ SFTP / FTP
                    ▼
            Servidor GK Software (import_channel)
```

Detalle de capas y contexto técnico: [openspec/project.md](openspec/project.md).

## 🛠️ Tecnologías

| Capa        | Tecnología                                  |
|-------------|---------------------------------------------|
| Backend     | Python 3.9+, Flask                          |
| ETL/datos   | Pandas, openpyxl                            |
| Base de datos | SQLite                                    |
| Transporte  | Paramiko (SFTP), ftplib (FTP)               |
| Scheduler   | `schedule` (hilo en segundo plano)          |
| Frontend    | Jinja2, TailwindCSS (CDN), jQuery           |

## 📦 Requisitos

- Python 3.9 o superior (probado en 3.12/3.13).
- `pip` y `venv`.
- Acceso de red al servidor GK (SFTP/FTP) para el envío.

## 🚀 Configuración

```bash
# 1. Clonar
git clone https://github.com/jalducin/dataMasterGK.git
cd dataMasterGK

# 2. Entorno virtual
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

# 3. Dependencias
pip install -r requirements.txt          # ejecución
pip install -r requirements-dev.txt      # + pruebas (pytest)

# 4. Configuración local (credenciales fuera del repo)
cp config.example.json config.json       # luego edita rutas y datos del servidor

# 5. Inicializar base de datos
python init_db.py

# 6. Levantar
python app.py                # panel en http://127.0.0.1:5000
```

`config.json` está en `.gitignore` (puede contener credenciales): el repositorio solo versiona
`config.example.json`. También puedes completar las rutas y el servidor SFTP/FTP desde el panel →
**Configuración**. La verificación de host key SFTP se controla con `host_key_policy`
(`advertir` por defecto, `verificar` para validar contra `~/.ssh/known_hosts`).

## ⚙️ Scripts y entradas

| Comando             | Para qué                                              |
|---------------------|-------------------------------------------------------|
| `python app.py`     | Levanta el panel web y el scheduler en segundo plano  |
| `python init_db.py` | Crea/verifica las tablas SQLite                       |
| `python -m pytest`  | Ejecuta la suite de pruebas (golden XML, pipeline, seguridad) |

Operaciones desde el panel: ejecución manual por interfaz, programación horaria, carga de Excel
(drag-and-drop), consulta y descarga de historial a Excel.

## 🧪 Pruebas y CI

Suite con `pytest` (`python -m pytest`):

- **Golden XML**: el XML generado para GK (Tiendas, Operadores, Promociones, Categorías) se compara
  byte a byte contra muestras conocidas en `tests/golden/` — el refactor no altera el formato.
- **Pipeline**: una sola pasada por archivo, auditoría sin duplicados y esquema correcto.
- **Seguridad**: los endpoints de consulta rechazan tipos fuera de la lista blanca.

No hay CI configurada todavía. Verificación manual: depositar un Excel de prueba en el directorio de
una interfaz, ejecutar desde el panel y confirmar XML generado, envío, archivado y registro en SQLite.

## 📁 Estructura

```
.
├── app.py                  # rutas Flask del panel
├── interface_runner.py     # despacho de ejecución manual por interfaz
├── scheduler.py            # ejecución programada por hora (hilo)
├── init_db.py              # inicialización del esquema SQLite
├── config.example.json     # plantilla de configuración (versionada, sin credenciales)
├── config.json             # configuración local con credenciales (NO versionada)
├── requirements.txt        # dependencias de ejecución (versiones fijadas)
├── requirements-dev.txt    # + pytest
├── src/
│   ├── utils.py            # logging, generadores XML, serialización, acceso SQLite
│   ├── transport.py        # transmisor SFTP/FTP reutilizable (1 conexión por corrida)
│   ├── log_database.py     # esquema SQLite + índices
│   └── classes/
│       ├── base.py         # InterfaceProcessor: pipeline ETL único compartido
│       └── {store,operator,promotion,promotion_category}.py  # solo generan XML
├── templates/              # Jinja2 (index + partials del panel)
├── static/                 # css/tailwind-vendor.css (local) + js/funciones.js
├── tests/                  # golden XML, pipeline y seguridad (pytest)
├── db/                     # base SQLite (no versionada)
├── logs/                   # logs en tiempo de ejecución (no versionados)
├── docs/                   # estándares (base, documentación, backend)
└── openspec/               # flujo SDD: project.md, specs/, changes/
```

## 🔗 Integración (XML GK)

La salida es XML conforme a los esquemas de GK Software:

- **Tiendas** → `BusinessUnitPackageDO`
- **Operadores** → `OperatorList`
- **Promociones / Categorías** → `PromotionImport`

Los XML se escriben en `<directorio>/xml/` y se transmiten al `pathUcon` configurado del servidor GK.

## 📚 Documentación

- [openspec/project.md](openspec/project.md) — contexto técnico y de dominio (fuente canónica).
- [docs/base-standards.md](docs/base-standards.md) — principios base y reglas OpenSpec.
- [docs/backend-standards.md](docs/backend-standards.md) — estándares Python/Flask.
- [docs/documentation-standards.md](docs/documentation-standards.md) — estándares de documentación.
- [openspec/changes/](openspec/changes/) — cambios en curso (incluye el rediseño de rendimiento).

## 🔄 Cómo contribuir (SDD)

El proyecto usa **Spec-Driven Development** sobre OpenSpec: la especificación es la fuente de verdad.

1. Crear rama `feature/[change-name]`.
2. Generar los artefactos del cambio en `openspec/changes/<cambio>/`:
   `proposal.md` (por qué) → `specs/**` (qué) → `design.md` (cómo) → `tasks.md` (pasos).
   Usar las plantillas en `openspec/schemas/spec-driven/templates/` o los comandos `/opsx:*`.
3. Implementar siguiendo las tasks; agregar/actualizar pruebas y documentación.
4. Verificar contra los artefactos (`/opsx:verify`) y abrir PR.
5. Al cerrar, archivar el cambio (`/opsx:archive`) y sincronizar specs.

## 📄 Licencia

Uso interno. Definir licencia según la política del repositorio.
