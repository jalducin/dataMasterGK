## Why

El pipeline ETL es lento e ineficiente por reprocesamiento masivo: una ejecución manual procesa cada
interfaz **dos veces** (`interface_runner.py`) y, en el caso de Tiendas, el mismo directorio se recorre
**tres veces** dentro de `Store.read_file_items` — hasta 6× el trabajo necesario. Además se abre una
conexión SFTP/FTP por cada XML y una conexión SQLite por cada fila insertada, lo que multiplica la
latencia con volúmenes reales. A esto se suman defectos de correctitud (inserción en una columna
inexistente), riesgos de seguridad (SQL con nombre de tabla interpolado desde la petición) y una
dependencia no apta para producción (Tailwind Play CDN). El objetivo es rediseñar el pipeline para que
sea correcto, idempotente y rápido sin cambiar el formato XML que GK espera.

## What Changes

- **BREAKING (interno)**: unificar el procesamiento en un pipeline único por archivo. Eliminar la doble
  invocación de `read_file_items` en `interface_runner.py` y los recorridos triplicados en `Store`.
- Reutilizar una sola conexión SFTP/FTP por corrida de interfaz en lugar de reconectar por XML.
- Reutilizar la conexión SQLite y usar transacciones/`executemany` por lote en lugar de abrir una
  conexión por fila.
- Corregir el esquema/inserciones: `Operator` inserta en la columna inexistente `login`; alinear
  `INSERT`/`UPDATE` con `src/log_database.py` y agregar índices de auditoría.
- Parametrizar/validar contra lista blanca los nombres de tabla en `/filtrar_fecha` y `/descargar_csv`
  (hoy interpolan `tipo` directamente en el SQL).
- Sustituir el manejo global de `sys.stdout` en `/ejecutar_stream` por una captura segura para hilos.
- Sacar credenciales del repositorio (config local ignorada por git) y definir política de host key SFTP.
- Actualizar dependencias *deprecated*: reemplazar Tailwind Play CDN por una build apta para producción.

## Capabilities

### New Capabilities
- `procesamiento-interfaces`: pipeline ETL único por archivo, idempotente y sin reprocesos, compartido por las cuatro interfaces.
- `transmision-archivos`: transmisión SFTP/FTP reutilizando una sola conexión por corrida, con política de host key.
- `persistencia-auditoria`: acceso a SQLite eficiente (conexión reutilizada, lotes) y esquema/inserciones consistentes.
- `seguridad-aplicacion`: consultas parametrizadas y por lista blanca, manejo de secretos fuera del repo y dependencias soportadas.

### Modified Capabilities
<!-- No hay specs vigentes en openspec/specs/; todas las capacidades son nuevas. -->

## Impact

- Código: `interface_runner.py`, `scheduler.py`, `app.py`, `src/utils.py`, `src/log_database.py`,
  `src/classes/{store,operator,promotion,promotion_category}.py`, `templates/index.html`.
- Datos: nuevas columnas/índices de auditoría en SQLite (compatibles hacia atrás vía `CREATE IF NOT EXISTS`).
- Configuración: `config.json` deja de versionarse con credenciales; se documenta `config.local.json`.
- Comportamiento externo: el formato XML enviado a GK **no cambia**; sí cambian rendimiento y robustez.
