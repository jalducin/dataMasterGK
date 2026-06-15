# seguridad-aplicacion Specification

## Purpose
TBD - created by archiving change redisenar-pipeline-etl. Update Purpose after archive.
## Requirements
### Requirement: Consultas con lista blanca de tablas
El sistema SHALL validar el parámetro de tipo/tabla de los endpoints de consulta (`/filtrar_fecha`,
`/descargar_csv`) contra una lista blanca fija de tablas y columnas permitidas. NO SHALL interpolar
nombres de tabla provenientes de la petición dentro del SQL.

#### Scenario: Tipo permitido
- **WHEN** se consulta el historial con un tipo incluido en la lista blanca
- **THEN** el sistema ejecuta una consulta predefinida y segura para ese tipo

#### Scenario: Tipo no permitido
- **WHEN** se consulta el historial con un tipo fuera de la lista blanca
- **THEN** el sistema rechaza la petición con un error de validación (HTTP 400) y no ejecuta SQL dinámico

### Requirement: Credenciales fuera del repositorio
El sistema SHALL leer las credenciales del servidor desde configuración local no versionada
(variables de entorno o `config.local.json` ignorado por git). El repositorio NO SHALL contener
credenciales reales.

#### Scenario: Configuración con credenciales
- **WHEN** se configuran usuario y contraseña del servidor GK
- **THEN** se almacenan en configuración local ignorada por git y no se incluyen en archivos versionados

### Requirement: Captura de salida segura para hilos
El sistema SHALL capturar la salida de una ejecución sin reasignar `sys.stdout` global, de modo que el
streaming de logs sea seguro frente a ejecuciones concurrentes y al scheduler en segundo plano.

#### Scenario: Streaming durante ejecución concurrente
- **WHEN** se transmite el log de una ejecución mientras otra corre en paralelo
- **THEN** cada stream recibe solo su propia salida, sin mezclarse ni perderse

### Requirement: Dependencias soportadas y reproducibles
El sistema SHALL evitar dependencias marcadas como no aptas para producción y SHALL fijar versiones de
las dependencias de Python. El frontend NO SHALL usar el Tailwind Play CDN en producción.

#### Scenario: Carga del panel
- **WHEN** se carga el panel web
- **THEN** los estilos provienen de una build/hoja apta para producción, no del Play CDN de Tailwind

#### Scenario: Instalación de dependencias
- **WHEN** se instalan las dependencias desde `requirements.txt`
- **THEN** las versiones quedan fijadas para una instalación reproducible

