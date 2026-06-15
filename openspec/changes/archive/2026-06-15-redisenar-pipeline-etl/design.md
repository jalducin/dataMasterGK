## Context

dataMasterGK funciona en producción pero "es lento" y arrastra deuda técnica acumulada por iteraciones
sucesivas (se ve código copiado-pegado y restos de versiones previas). Este documento cataloga la deuda
detectada en una auditoría del código y define el rediseño. **Restricción dura**: el XML resultante para
GK Software no debe cambiar; el rediseño es interno (rendimiento, correctitud, seguridad,
mantenibilidad).

### Catálogo de deuda técnica detectada

**A. Reprocesamiento (causa principal de lentitud)**
- `interface_runner.py`: cada interfaz ejecuta `read_file_items()` **dos veces** (una fuera y otra
  dentro del `try`). Trabajo y envíos duplicados.
- `src/classes/store.py`: `read_file_items` contiene **tres** bloques casi idénticos que recorren el
  mismo directorio (líneas ~30, ~68 y ~104). Combinado con lo anterior, Tiendas llega a procesarse hasta 6×.
- Efecto: relectura de Excel, regeneración de XML y reintentos de envío redundantes.

**B. Conexiones por elemento (latencia que escala con el volumen)**
- `utils.send_item_files`: abre y cierra una conexión SFTP/FTP **por cada XML**. Con N archivos = N
  handshakes. Es el cuello de botella real cuando hay muchas tiendas/operadores.
- `log_to_db` (las cuatro clases) y `utils.register_xml_log`: abren una conexión SQLite **por fila** y,
  además, registran el XML dos veces (en `_create_*` y de nuevo en la fase de envío).

**C. Correctitud**
- `Operator.create_files`: inserta en `Operadores (codigo, login)` pero la tabla no tiene columna `login`
  (esquema real: `codigo, nombre, ubicacion, estado, fecha`). El `INSERT` falla y se traga en el `except`.
- `interface_runner.py`: el doble llamado provoca doble registro de ejecución y doble envío.
- Manejo de excepciones demasiado amplio (`except Exception`) que oculta errores reales.

**D. Seguridad**
- `app.py /filtrar_fecha` y `/descargar_csv`: `SELECT * FROM {tipo}` interpola el nombre de tabla desde
  la query string → riesgo de inyección. Debe validarse contra lista blanca.
- `config.json` versionado con campos de credenciales del servidor (hoy vacíos, pero el patrón filtra
  secretos). Debe salir del repo.
- `utils.send_item_files` (SFTP) no define política de verificación de host key.

**E. Concurrencia**
- `app.py /ejecutar_stream`: reasigna `sys.stdout` global para capturar prints. No es seguro con el
  scheduler en segundo plano ni con peticiones concurrentes; puede mezclar/perder salida.

**F. Dependencias / deprecated**
- `templates/index.html`: Tailwind **Play CDN** (`cdn.tailwindcss.com`), que el propio proyecto Tailwind
  marca como "no usar en producción". Reemplazar por build/CLI o CSS compilado.
- `requirements.txt` sin versiones fijadas (instalaciones no reproducibles).

**G. Mantenibilidad**
- El bloque "procesar → esperar archivo → enviar → archivar → auditar" está copiado en las cuatro clases
  con variaciones menores. Debe vivir en una sola base/función reutilizable.

## Goals / Non-Goals

**Goals:**
- Eliminar todo reprocesamiento: una lectura, una generación y un envío por archivo.
- Reutilizar conexiones (SFTP/FTP y SQLite) por corrida.
- Corregir inserciones y alinear el esquema; auditar una sola vez por XML.
- Cerrar la inyección SQL y sacar secretos del repositorio.
- Reemplazar dependencias *deprecated*.
- Extraer el pipeline común a una base compartida por las cuatro interfaces.

**Non-Goals:**
- Cambiar el formato XML que recibe GK.
- Migrar de SQLite a otro motor, o de Flask a otro framework.
- Rediseñar la UI más allá de sustituir el CDN deprecated.
- Reescribir el scheduler (solo se beneficia del pipeline corregido).

## Decisions

1. **Pipeline único e idempotente por archivo.** Una función/base `procesar_directorio` ejecuta el ciclo
   completo una sola vez: detectar Excel → leer → generar XML → enviar (conexión compartida) → archivar
   (enviadas/no_enviadas) → mover Excel (procesados/no_procesados) → auditar.
   *Alternativa descartada*: parchear los bucles duplicados in situ; mantiene la duplicación y el riesgo.

2. **Clase base `InterfaceProcessor`.** Las cuatro clases heredan el pipeline y solo implementan
   `generar_xml(fila|hojas) -> (xml_path, name_file, id)`. Elimina el copy-paste (deuda G).
   *Alternativa*: funciones sueltas en utils; se prefiere base por el estado compartido (config, db_path).

3. **Conexión SFTP/FTP reutilizada por corrida.** `send_item_files` se divide en `abrir_conexion()` /
   `enviar(conn, ...)` / `cerrar()`, y el pipeline envía el lote con una sola conexión.
   *Trade-off*: hay que manejar reconexión ante caída a mitad de lote → reintento acotado por archivo.

4. **Acceso SQLite por unidad de trabajo.** Una conexión por corrida y `executemany`/transacción para
   inserciones de auditoría. Registrar el XML **una sola vez**.

5. **Lista blanca de tablas consultables.** `/filtrar_fecha` y `/descargar_csv` mapean `tipo` a un
   conjunto fijo de tablas/columnas permitidas; cualquier otro valor → 400. Sin interpolar nombres.

6. **Secretos fuera del repo.** `config.json` versionado solo con rutas y `config.example.json` de
   referencia; credenciales reales en `config.local.json` (en `.gitignore`) o variables de entorno.

7. **Captura de salida segura para hilos.** Sustituir la reasignación global de `sys.stdout` por
   `contextlib.redirect_stdout` sobre un buffer local, o emitir eventos directamente al stream SSE.

8. **Dependencias soportadas y fijadas.** Reemplazar Tailwind Play CDN por una hoja compilada/servida
   localmente; fijar versiones mínimas en `requirements.txt`.

## Risks / Trade-offs

- [Regresión del XML al refactorizar] → Mitigación: pruebas de "golden file" que comparen el XML generado
  contra muestras conocidas buenas antes/después.
- [Reutilizar conexión SFTP oculta fallos intermitentes por archivo] → Mitigación: reintento acotado por
  archivo y registro individual de estado enviado/pendiente.
- [Cambiar `config.json` rompe instalaciones existentes] → Mitigación: arranque que detecte ausencia de
  `config.local.json` y migre/instruya; mantener compatibilidad de lectura.
- [Reemplazo del CDN cambia estilos] → Mitigación: compilar con las mismas clases usadas y revisar el panel.

## Migration Plan

1. Introducir la base `InterfaceProcessor` y migrar una interfaz (Operator) como piloto con pruebas golden.
2. Migrar las tres restantes y eliminar bucles/llamados duplicados.
3. Refactorizar `send_item_files` a conexión reutilizable; ajustar el pipeline.
4. Ajustar acceso SQLite (conexión por corrida, índices) y corregir columnas.
5. Endurecer endpoints (lista blanca) y mover secretos.
6. Reemplazar el CDN y fijar dependencias.
7. Verificación manual + pruebas; archivar el cambio.

Rollback: cada paso es un commit aislado en `feature/redisenar-pipeline-etl`; revertir por commit.

## Open Questions

- ¿Volumen real máximo de filas por Excel y de archivos por corrida? (dimensiona el lote SQLite y el timeout SFTP).
- ¿El servidor GK exige host key fija que podamos fijar en `known_hosts`?
- ¿Se requiere mantener soporte FTP plano o ya todo es SFTP?
