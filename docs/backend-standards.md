---
description: Estándares de backend (Python/Flask) para dataMasterGK. Aplican a todo el código del servidor, el pipeline ETL y el acceso a datos.
alwaysApply: true
---

# Estándares de backend — Python / Flask

> Complementan a `base-standards.md`. Aplican a `app.py`, `scheduler.py`, `interface_runner.py`,
> `src/` y cualquier código de servidor de dataMasterGK.

## 1. Estilo y estructura

- **PEP 8** y formateo consistente. Una responsabilidad por función.
- **Imports al inicio del módulo.** No usar `import` dentro de funciones salvo dependencias opcionales
  o para romper ciclos justificados (documentarlo).
- **Type hints** en firmas públicas y contratos de datos (`def f(x: str) -> bool`).
- **Sin código duplicado**: si la misma lógica aparece en varias clases de interfaz, extraerla a una
  base común o a `utils`. El pipeline de procesar → enviar → archivar → auditar debe vivir en un solo lugar.

## 2. Acceso a datos (SQLite)

- **Reutilizar la conexión** dentro de una unidad de trabajo; no abrir una conexión nueva por cada fila.
  Para lotes, usar `executemany` o una sola transacción.
- **Siempre parametrizar** las consultas con placeholders (`?`). **Nunca** interpolar valores ni nombres
  de tabla/columna provenientes de entrada del usuario en el SQL. Si el nombre de tabla es dinámico,
  validarlo contra una lista blanca explícita.
- El esquema es la fuente de verdad: toda columna usada en `INSERT/UPDATE` debe existir en
  `src/log_database.py`. Mantenerlos sincronizados.
- Definir índices para las columnas usadas en filtros frecuentes (`fecha`, `interface`).

## 3. Pipeline ETL e idempotencia

- **Un solo paso de procesamiento por archivo**: leer el Excel una vez, generar el XML una vez, enviar
  una vez. Prohibido reprocesar el mismo archivo o re-leer el Excel en bucles repetidos.
- El procesamiento debe ser **idempotente y atómico** por archivo: éxito → mover a procesados;
  error → mover a no procesados; nunca dejar archivos a medio procesar.
- **Reutilizar la conexión SFTP/FTP** para enviar varios XML de una misma corrida; no reconectar por archivo.

## 4. Manejo de errores y logging

- No capturar `Exception` para silenciarla. Capturar lo específico, registrar contexto accionable y
  decidir explícitamente si se continúa o se aborta.
- Logging por el módulo `logging` con niveles correctos (INFO/WARNING/ERROR). Evitar `print` para
  telemetría; reservar la captura de `stdout` para casos acotados y nunca reasignar `sys.stdout` global
  en código concurrente.
- Todo error operativo relevante debe quedar auditado en SQLite además del log en archivo.

## 5. Configuración y secretos

- **No** versionar credenciales. Las contraseñas y datos de servidor van en configuración local
  (variables de entorno o `config.json` ignorado por git), nunca en el repositorio.
- Validar la configuración al cargarla; fallar con un mensaje claro si falta un campo requerido.
- Verificación de host key en SFTP: definir política explícita (no aceptar claves desconocidas en
  producción sin justificación).

## 6. Concurrencia

- El scheduler corre en un hilo en segundo plano: el estado compartido (p. ej. `sys.stdout`,
  conexiones) debe ser seguro frente a ejecuciones manuales simultáneas desde el panel.

## 7. Pruebas

- Cuando se agregue una capacidad, agregar pruebas que verifiquen los escenarios del spec
  (generación de XML, parseo de Excel, ruteo de archivos). Preferir TDD cuando aplique.
