## ADDED Requirements

### Requirement: Procesamiento único por archivo
El sistema SHALL procesar cada archivo Excel exactamente una vez por corrida: una sola lectura, una sola
generación de XML y un solo intento de envío. NO SHALL recorrer el mismo directorio ni invocar el
procesamiento de una interfaz más de una vez por corrida.

#### Scenario: Ejecución manual de una interfaz
- **WHEN** un usuario dispara la ejecución manual de una interfaz desde el panel
- **THEN** cada Excel del directorio se lee una sola vez y se genera/envía un único conjunto de XML, sin duplicados

#### Scenario: Directorio con múltiples Excel
- **WHEN** el directorio de una interfaz contiene varios archivos `.xlsx`
- **THEN** cada archivo se procesa una vez y de forma independiente, sin reprocesar los ya atendidos en la misma corrida

### Requirement: Pipeline idempotente y atómico por archivo
El sistema SHALL completar el ciclo procesar → enviar → archivar de forma atómica por archivo: en éxito,
el Excel se mueve a procesados; en error, a no procesados. NO SHALL dejar archivos a medio procesar.

#### Scenario: Archivo procesado con éxito
- **WHEN** un Excel se transforma y todos sus XML se gestionan sin excepción
- **THEN** el Excel se mueve a la carpeta de procesados y los XML quedan archivados en `enviadas`/`no_enviadas` según su estado

#### Scenario: Archivo con error de transformación
- **WHEN** la transformación de un Excel lanza una excepción
- **THEN** el Excel se mueve a la carpeta de no procesados y el error queda registrado en log y en SQLite

### Requirement: Pipeline compartido entre interfaces
El sistema SHALL implementar el ciclo procesar → enviar → archivar → auditar en un único componente
reutilizable; cada interfaz SHALL aportar solo su lógica de transformación a XML.

#### Scenario: Alta de una nueva interfaz
- **WHEN** se agrega una interfaz nueva
- **THEN** reutiliza el pipeline común y solo implementa su generación de XML, sin duplicar la lógica de envío/archivado/auditoría
