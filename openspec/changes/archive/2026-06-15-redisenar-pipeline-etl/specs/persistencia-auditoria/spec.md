## ADDED Requirements

### Requirement: Acceso a SQLite por unidad de trabajo
El sistema SHALL reutilizar una conexión SQLite por corrida y SHALL agrupar las inserciones de auditoría
en transacciones o `executemany`. NO SHALL abrir una conexión nueva por cada fila insertada.

#### Scenario: Auditoría de una corrida con muchas filas
- **WHEN** una corrida genera muchos registros de auditoría
- **THEN** se insertan en una o pocas transacciones reutilizando la misma conexión, no una conexión por fila

### Requirement: Inserciones consistentes con el esquema
El sistema SHALL usar en cada `INSERT`/`UPDATE` solo columnas existentes en el esquema definido en
`src/log_database.py`. Las inserciones NO SHALL fallar silenciosamente por columnas inexistentes.

#### Scenario: Registro de un operador procesado
- **WHEN** se registra un operador en la tabla `Operadores`
- **THEN** la inserción usa únicamente columnas existentes del esquema y se completa sin error de columna desconocida

### Requirement: Auditoría única por XML
El sistema SHALL registrar cada XML generado una sola vez en `XML_Generados`, actualizando su estado
(Pendiente → Enviado/Fallido) en lugar de crear registros duplicados.

#### Scenario: Generación y envío de un XML
- **WHEN** un XML se genera y luego se transmite
- **THEN** existe un único registro en `XML_Generados` cuyo estado refleja el resultado final del envío

### Requirement: Índices de auditoría
El sistema SHALL definir índices sobre las columnas usadas en filtros frecuentes de las tablas de
auditoría (al menos `fecha` y, donde aplique, `interface`/`tipo`).

#### Scenario: Consulta de historial por fecha
- **WHEN** el panel consulta registros filtrando por fecha
- **THEN** la consulta se apoya en un índice por fecha y no realiza un escaneo completo de tabla
