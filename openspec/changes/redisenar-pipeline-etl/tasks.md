## 0. Preparación

- [x] 0.1 Crear y cambiar a la feature branch `feature/redisenar-pipeline-etl` (SIEMPRE PRIMERO)
- [x] 0.2 Crear muestras "golden" del XML actual (Store, Operator, Promotion, Promotion Category) para comparar antes/después

## 1. Eliminar reprocesamiento (rendimiento)

- [x] 1.1 Reescribir `interface_runner.py` para invocar `read_file_items()` una sola vez por interfaz, con registro de ejecución único
- [x] 1.2 Eliminar los recorridos duplicados/triplicados de directorio en `src/classes/store.py`, dejando un solo paso
- [x] 1.3 Verificar que `scheduler.py` use el mismo pipeline único

## 2. Pipeline compartido (mantenibilidad)

- [x] 2.1 Crear base `InterfaceProcessor` con el ciclo procesar → enviar → archivar → auditar
- [x] 2.2 Migrar `Operator` a la base como piloto y validar contra el golden file
- [x] 2.3 Migrar `Store`, `Promotion` y `PromotionCategory` a la base; cada clase solo aporta `generar_xml`

## 3. Transmisión SFTP/FTP (rendimiento)

- [x] 3.1 Dividir `utils.send_item_files` en abrir/enviar/cerrar con conexión reutilizable (`src/transport.py`)
- [x] 3.2 Enviar el lote de XML de una corrida con una sola conexión y reintento acotado por archivo
- [x] 3.3 Definir política de verificación de host key SFTP (configurable: `advertir`/`verificar`)

## 4. Persistencia y auditoría (rendimiento + correctitud)

- [x] 4.1 Reutilizar una conexión SQLite por corrida y commitear una sola vez
- [x] 4.2 Corregir el `INSERT` de `Operator` para usar solo columnas existentes en `src/log_database.py`
- [x] 4.3 Registrar cada XML una sola vez en `XML_Generados` con su estado final (sin duplicados)
- [x] 4.4 Agregar índices por `fecha` (y `tipo`) en las tablas de auditoría

## 5. Seguridad

- [x] 5.1 Reemplazar el SQL dinámico de `/filtrar_fecha` y `/descargar_csv` por consultas con lista blanca
- [x] 5.2 Sustituir la reasignación global de `sys.stdout` en `/ejecutar_stream` por `contextlib.redirect_stdout`
- [x] 5.3 Mover credenciales fuera del repo (`config.json` ignorado; `config.example.json` versionado)

## 6. Dependencias deprecated

- [x] 6.1 Reemplazar el Tailwind Play CDN de `templates/index.html` por una hoja local (`static/css/tailwind-vendor.css`)
- [x] 6.2 Fijar versiones en `requirements.txt` (+ `requirements-dev.txt`)

## 7. Pasos obligatorios

- [x] 7.1 (OBLIGATORIO) Pruebas: golden tests del XML por interfaz + pipeline + lista blanca de endpoints
- [x] 7.2 (OBLIGATORIO) Ejecutar pruebas y suite — EL AGENTE EJECUTA — reporte en `specs/redisenar-pipeline-etl/reports/2026-06-14-step-7.2-pruebas-y-verificacion.md` (10/10 PASS)
- [x] 7.3 (OBLIGATORIO) Verificación manual (ETL + HTTP) — EL AGENTE EJECUTA: app arrancada, endpoints 200/400 verificados, pipeline e2e con dirs temporales; estado restaurado
- [x] 7.4 (OBLIGATORIO) Actualizar documentación: `README.md`, `openspec/project.md`, `docs/backend-standards.md`, `config.example.json`
