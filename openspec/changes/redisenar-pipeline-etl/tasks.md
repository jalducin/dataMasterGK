## 0. Preparación

- [ ] 0.1 Crear y cambiar a la feature branch `feature/redisenar-pipeline-etl` (SIEMPRE PRIMERO)
- [ ] 0.2 Crear muestras "golden" del XML actual (Store, Operator, Promotion, Promotion Category) para comparar antes/después

## 1. Eliminar reprocesamiento (rendimiento)

- [ ] 1.1 Reescribir `interface_runner.py` para invocar `read_file_items()` una sola vez por interfaz, con registro de ejecución único
- [ ] 1.2 Eliminar los recorridos duplicados/triplicados de directorio en `src/classes/store.py`, dejando un solo paso
- [ ] 1.3 Verificar que `scheduler.py` use el mismo pipeline único

## 2. Pipeline compartido (mantenibilidad)

- [ ] 2.1 Crear base `InterfaceProcessor` con el ciclo procesar → enviar → archivar → auditar
- [ ] 2.2 Migrar `Operator` a la base como piloto y validar contra el golden file
- [ ] 2.3 Migrar `Store`, `Promotion` y `PromotionCategory` a la base; cada clase solo aporta `generar_xml`

## 3. Transmisión SFTP/FTP (rendimiento)

- [ ] 3.1 Dividir `utils.send_item_files` en abrir/enviar/cerrar con conexión reutilizable
- [ ] 3.2 Enviar el lote de XML de una corrida con una sola conexión y reintento acotado por archivo
- [ ] 3.3 Definir política de verificación de host key SFTP (configurable)

## 4. Persistencia y auditoría (rendimiento + correctitud)

- [ ] 4.1 Reutilizar una conexión SQLite por corrida y usar `executemany`/transacción para auditoría
- [ ] 4.2 Corregir el `INSERT` de `Operator` para usar solo columnas existentes en `src/log_database.py`
- [ ] 4.3 Registrar cada XML una sola vez en `XML_Generados` y actualizar su estado (sin duplicados)
- [ ] 4.4 Agregar índices por `fecha` (y `tipo`/`interface` donde aplique) en las tablas de auditoría

## 5. Seguridad

- [ ] 5.1 Reemplazar el SQL dinámico de `/filtrar_fecha` y `/descargar_csv` por consultas con lista blanca de tablas/columnas
- [ ] 5.2 Sustituir la reasignación global de `sys.stdout` en `/ejecutar_stream` por captura segura para hilos
- [ ] 5.3 Mover credenciales a `config.local.json` (ignorado por git) o variables de entorno; agregar `config.example.json`

## 6. Dependencias deprecated

- [ ] 6.1 Reemplazar el Tailwind Play CDN de `templates/index.html` por una build/hoja apta para producción
- [ ] 6.2 Fijar versiones mínimas en `requirements.txt`

## 7. Pasos obligatorios

- [ ] 7.1 (OBLIGATORIO) Revisar y crear pruebas: golden tests del XML por interfaz + pruebas de lista blanca de endpoints (TDD donde aplique)
- [ ] 7.2 (OBLIGATORIO) Ejecutar las pruebas dirigidas y la suite — EL AGENTE EJECUTA — y crear el reporte en `specs/redisenar-pipeline-etl/reports/AAAA-MM-DD-step-7.2-pruebas-y-verificacion.md`
- [ ] 7.3 (OBLIGATORIO) Verificación manual (ETL + HTTP) — EL AGENTE EJECUTA: depositar un Excel de prueba, ejecutar la interfaz, verificar XML idéntico al golden, envío, archivado y auditoría única; probar `/filtrar_fecha` con tipo válido e inválido; restaurar el estado de datos al terminar
- [ ] 7.4 (OBLIGATORIO) Actualizar documentación: `README.md`, `openspec/project.md`, `docs/backend-standards.md` y `config.example.json`, manteniendo consistencia y sin enlaces rotos
