---
description: Reglas y guías de desarrollo del proyecto, aplicables a todos los agentes de IA (Claude, Gemini, etc.).
alwaysApply: true
---

# Estándares base

> Plantilla SDD (Spec-Driven Development). Este documento es genérico y sirve para cualquier
> stack (Python, PHP, React, n8n, etc.). Ajusta lo que sea específico de tu proyecto al instanciarla.

## 1. Principios fundamentales

- **Tareas pequeñas, una a la vez**: avanzar en pasos cortos. No saltar más de un paso a la vez.
- **Desarrollo guiado por pruebas (TDD)**: cuando aplique, empezar por una prueba que falle antes de implementar.
- **Seguridad de tipos / contratos claros**: tipar el código cuando el lenguaje lo permita; definir contratos explícitos.
- **Nombres claros**: nombres descriptivos para variables, funciones y artefactos.
- **Cambios incrementales**: preferir cambios pequeños y enfocados sobre modificaciones grandes y complejas.
- **Cuestionar supuestos**: validar suposiciones e inferencias antes de implementar.
- **Detección de patrones**: identificar y señalar código repetido.

## 2. Estándar de idioma

- **Documentación y comentarios: español.** README, guías, comentarios de código, mensajes de commit,
  descripciones de pruebas y artefactos OpenSpec se redactan en español.
- **Identificadores de código** (variables, funciones, clases): seguir la convención idiomática del
  lenguaje del proyecto (habitualmente inglés). La coherencia dentro del proyecto manda.
- Cada proyecto puede ajustar esta regla en su propio `docs/*-standards.md` si lo necesita.

## 3. Estándares específicos

Para guías detalladas por área, cada proyecto agrega sus documentos en `docs/`:

- `docs/<area>-standards.md` — por ejemplo `backend-standards.md`, `frontend-standards.md`,
  `sql-standards.md`, `n8n-standards.md`, según el stack del proyecto.
- `docs/documentation-standards.md` — estructura, formato y mantenimiento de la documentación.

Crea solo los que apliquen a tu proyecto y enlázalos desde `openspec/config.yaml`.

## 4. Skills del proyecto

- Las skills viven en `ai-specs/skills`.
- Cuando una petición coincide con una skill, cargar y seguir su `SKILL.md` automáticamente antes de continuar.
- Cargar también los archivos referenciados por la skill (por ejemplo `references/*.md`) cuando los requiera.

## 5. Requisito de modelo para planificación

Los flujos de planificación deben ejecutarse con un modelo de razonamiento alto (Opus high reasoning).

Aplica a:
- `enrich-us`
- `openspec-ff-change`
- `openspec-continue-change`

Antes de iniciar cualquiera de estos flujos, verificar que la sesión usa un modelo de razonamiento alto.
Si no, **autocorregir** ajustando el modelo en la configuración del agente (`.claude/settings.json` para Claude),
y continuar — no detenerse a preguntar. Volver al modelo estándar para el resto de pasos.

## 6. Integridad de referencias y portabilidad multi-agente

- **Fuente canónica**: mantener los artefactos reutilizables en `ai-specs` como fuente única. Las rutas
  específicas por agente (`.claude`, `.gemini`) referencian esa fuente (symlink donde el SO lo permita;
  copia real en Windows).
- **Seguridad al actualizar**: al renombrar o mover un archivo, verificar y actualizar todas las
  referencias/symlinks que lo apunten antes de dar el cambio por terminado.
- **Enlazado de artefactos nuevos**: al crear un artefacto que requiere exposición multi-agente (agentes o
  skills en `ai-specs`), crear las referencias correspondientes desde las rutas de cada agente.
- **Revisión de personalización externa**: si se introduce personalización fuera de `ai-specs`, evaluar
  moverla a `ai-specs` y referenciarla desde su ubicación original.
- **Compuerta de cierre**: un cambio está incompleto si deja referencias rotas, destinos obsoletos o
  artefactos canónicos duplicados entre carpetas de agentes.

## 7. Actualización obligatoria de artefactos OpenSpec en cambios post-apply

Cuando aparece un nuevo arreglo/cambio después de `opsx:apply` (o `/apply`) y antes de `opsx:archive`
(o `/archive`), debe tratarse como una actualización del spec primero, no como un "arréglalo rápido".
Es el principio central de OpenSpec: la documentación es la fuente de verdad.

Orden requerido:

1. Actualizar los artefactos del cambio OpenSpec afectados (escenarios, requisitos/specs y `tasks.md`).
   No agregar tareas como "bugfixes" sueltos, sino como parte del diseño en la sección correspondiente.
2. Si hace falta regenerar artefactos, ejecutar el paso OpenSpec correspondiente (`opsx:continue`, `opsx:ff`
   o equivalente) antes de codificar.
3. Implementar código solo después de que los artefactos reflejen la nueva petición.
4. Re-ejecutar la verificación contra los artefactos actualizados antes de archivar.

No aplicar arreglos directos solo en código dentro de esa ventana sin actualizar los artefactos OpenSpec.
