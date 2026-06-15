## ADDED Requirements

### Requirement: Conexión de transporte reutilizada por corrida
El sistema SHALL abrir una sola conexión SFTP/FTP por corrida de interfaz y reutilizarla para transmitir
todos los XML generados. NO SHALL abrir una conexión nueva por cada archivo.

#### Scenario: Envío de varios XML en una corrida
- **WHEN** una corrida genera varios XML para la misma interfaz
- **THEN** todos se transmiten reutilizando una única conexión, que se cierra al terminar el lote

#### Scenario: Cierre de conexión al finalizar
- **WHEN** termina el envío del lote (con o sin errores)
- **THEN** la conexión de transporte se cierra de forma explícita, sin dejar sesiones abiertas

### Requirement: Reintento acotado y registro por archivo
El sistema SHALL registrar el resultado de envío (enviado/pendiente) por cada XML y SHALL reintentar de
forma acotada ante fallos transitorios sin abortar el lote completo.

#### Scenario: Fallo transitorio en un archivo
- **WHEN** la transmisión de un XML falla por un error transitorio
- **THEN** el sistema reintenta un número acotado de veces y, si persiste, marca ese XML como pendiente y continúa con los demás

### Requirement: Política de verificación de host key SFTP
El sistema SHALL definir una política explícita de verificación de host key al conectar por SFTP.

#### Scenario: Conexión SFTP a host configurado
- **WHEN** se establece una conexión SFTP con el servidor GK
- **THEN** la identidad del host se valida según la política configurada en lugar de aceptarse de forma implícita
