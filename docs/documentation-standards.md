---
description: Estándares y buenas prácticas para la documentación técnica del proyecto (estructura, proceso de actualización y reglas de idioma).
alwaysApply: true
---

# Reglas y patrones de documentación y AI specs

## Introducción

La **documentación técnica** abarca todo lo que describe cómo está estructurado, se ejecuta y opera el
proyecto: modelo de datos, README, especificaciones de API/contratos, y demás documentos `.md`.

Las **AI specs** son los documentos que explican a los agentes de IA cómo comportarse, documentar,
planificar y codificar: acuerdos de equipo, estándares y convenciones.

## Reglas generales

- **Idioma: español** para documentación y comentarios (ver `base-standards.md` §2). Los identificadores
  de código siguen la convención del lenguaje.
- La documentación debe ser precisa, estar formateada de forma consistente y reflejar el estado real del código.

## Documentación técnica

Antes de cualquier commit o push —o cuando se pida documentar un cambio— SIEMPRE revisar qué documentación
técnica debe actualizarse:

1. Revisar los cambios recientes en el código.
2. Identificar qué archivos de documentación necesitan actualización. Ejemplos:
   - Cambios de modelo de datos → actualizar el documento de modelo de datos del proyecto.
   - Cambios de API/contratos → actualizar la especificación de API correspondiente.
   - Cambios de librerías, migraciones o proceso de instalación → actualizar el `docs/*-standards.md` que aplique.
3. Actualizar cada archivo afectado manteniendo consistencia con la documentación existente.
4. Verificar que los cambios queden reflejados con exactitud.
5. Reportar qué archivos se actualizaron y qué cambió.

## AI specs

Proceso para que la IA aprenda de la retroalimentación del usuario y mejore los estándares del proyecto:

- Aprender de la retroalimentación, guía y sugerencias durante las interacciones.
- Identificar proactivamente oportunidades para mejorar las reglas existentes.
- Mantener la asistencia alineada con las necesidades cambiantes del proyecto.

Aplica después de cualquier interacción con retroalimentación explícita o implícita.

### Anti-patrones a evitar

- **Saltarse la aprobación**: aplicar cambios a las reglas sin revisión y aprobación explícita del usuario.
- **Propuestas sin vínculo**: proponer cambios de reglas sin conectarlos con la retroalimentación concreta.
- **Modificaciones imprecisas**: no señalar qué regla o sección específica cambia.
- **Retroalimentación no atendida**: no iniciar el proceso de mejora cuando el usuario da retroalimentación relevante.
- **Scope creep**: actualizar múltiples reglas no relacionadas a la vez.
- **Cambios no solicitados**: modificar reglas sin conexión con retroalimentación. Las actualizaciones son reactivas.
- **Falta de confirmación**: no avisar al usuario tras implementar una modificación aprobada.
