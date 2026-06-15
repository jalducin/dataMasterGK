# Contexto del proyecto

Este documento da contexto a los agentes de IA sobre el proyecto dataMasterGK.

## Qué es

dataMasterGK es un middleware ETL (Extract, Transform, Load) que automatiza la integración de
datos maestros hacia sistemas **GK Software**. Monitorea directorios locales, procesa archivos
Excel de cuatro interfaces (Tiendas, Operadores, Promociones y Promociones por Categoría), los
transforma al formato XML estándar de GK y los transmite a un servidor central por SFTP/FTP.
Incluye un panel web para configurar, operar y auditar el proceso.

## Stack tecnológico

- Lenguaje: Python 3.9+ (probado en 3.12/3.13).
- Framework web: Flask.
- Base de datos: SQLite (auditoría de logs, XML generados, programación y ejecuciones).
- ETL/datos: Pandas + openpyxl (lectura de Excel).
- Transporte: Paramiko (SFTP) y ftplib (FTP).
- Scheduler: librería `schedule` en un hilo en segundo plano.
- Frontend: HTML + Jinja2, TailwindCSS (CDN), jQuery, multiple-select.

## Arquitectura

Aplicación monolítica Flask con un pipeline ETL por interfaz:

```
Excel (directorio vigilado) → lectura Pandas → transformación a XML (clase por interfaz)
   → escritura local del XML → envío SFTP/FTP → archivado (enviadas/no_enviadas) → auditoría SQLite
```

- `app.py`: rutas HTTP del panel (configuración, ejecución manual, streaming de logs, descargas).
- `scheduler.py`: ejecución programada por hora según `ProgramacionInterfaces`.
- `interface_runner.py`: despacho de ejecución manual por nombre de interfaz.
- `src/classes/base.py`: `InterfaceProcessor`, el pipeline ETL único y compartido (procesar →
  enviar con una sola conexión → auditar una vez → archivar de forma idempotente).
- `src/classes/{store,operator,promotion,promotion_category}.py`: una clase por interfaz; solo
  implementan `generar()` (leer Excel → construir XML + fila maestra).
- `src/transport.py`: `Transmisor` SFTP/FTP reutilizable (una conexión por corrida, reintento
  acotado, política de host key).
- `src/utils.py`: utilidades transversales (logging, generadores y serialización XML, acceso a SQLite).
- `src/log_database.py`: definición del esquema SQLite e índices de auditoría.

## Convenciones

- Idioma: documentación y comentarios en español; identificadores de código en inglés/español
  según el código existente (la coherencia dentro del proyecto manda).
- Commits: conventional commits.
- Ramas: `feature/[change-name]`.
- Estándares por área en `docs/*-standards.md` (ver `docs/backend-standards.md`).

## Comandos clave

- Crear entorno: `python -m venv venv` y activar (`venv\Scripts\activate` en Windows).
- Instalar dependencias: `pip install -r requirements.txt` (y `requirements-dev.txt` para pruebas)
- Configuración local: `cp config.example.json config.json` y completar credenciales
- Inicializar base de datos: `python init_db.py`
- Levantar el proyecto: `python app.py` (panel en `http://127.0.0.1:5000`)
- Pruebas: `python -m pytest` (golden XML, pipeline, seguridad)
