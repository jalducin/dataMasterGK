---
description: Pasos obligatorios al crear/actualizar tasks.md en cambios OpenSpec, agnósticos de tecnología. El agente ejecuta la verificación, no la delega.
alwaysApply: true
---

# Tasks OpenSpec: pasos obligatorios

Al crear o actualizar artefactos `tasks.md` en cambios OpenSpec, DEBES incluir los pasos obligatorios
de esta regla, en el orden correcto. Es **agnóstica de tecnología**: sirve para Python, PHP, React, n8n,
SQL, CLIs, etc. Cada proyecto especializa el "cómo" de la verificación según su stack.

## 1. Leer openspec/config.yaml primero

ANTES de crear o actualizar cualquier `tasks.md`, lee `openspec/config.yaml` para entender el stack,
las convenciones de ramas, la estructura de tareas y los requisitos de pruebas/documentación del proyecto.

## 2. Pasos obligatorios (en orden)

### Step 0 — Crear feature branch (SIEMPRE PRIMERO)
- Debe ser el primer paso.
- Convención: `feature/[ticket-id]` o `feature/[change-name]`.
- Crear y cambiar a la rama antes de cualquier modificación de código/artefacto.

### Step N — Revisar y actualizar pruebas existentes (OBLIGATORIO)
- Identificar y actualizar las pruebas impactadas por el cambio.
- Si el proyecto sigue TDD, escribir primero las pruebas que fallan.

### Step N+1 — Ejecutar pruebas y verificar estado (OBLIGATORIO)
**Responsabilidad del agente**: el agente ejecuta las pruebas él mismo; NUNCA delega al usuario.
1. Capturar el estado relevante previo (datos, conteos, snapshots) si el cambio muta estado.
2. Ejecutar pruebas dirigidas del módulo cambiado y confirmar que no hay regresiones.
3. Ejecutar la suite más amplia requerida por el proyecto.
4. Verificar el estado posterior y restaurarlo si las pruebas lo modificaron.
5. Crear el reporte en `specs/<change-name>/reports/AAAA-MM-DD-step-N+1-pruebas-y-verificacion.md`.
6. Marcar el paso completo solo tras pasar las pruebas y existir el reporte.

### Step N+2 — Verificación manual según el tipo de proyecto (OBLIGATORIO) — EL AGENTE EJECUTA
Elegir el método que corresponda al stack del cambio y **ejecutarlo el agente**, documentando comandos y resultados:
- **API/HTTP**: probar endpoints (p. ej. `curl`), verificar status y cuerpo; restaurar estado tras CREATE/UPDATE/DELETE.
- **CLI/script**: ejecutar el comando con casos válidos e inválidos y verificar salida y códigos de retorno.
- **UI/frontend**: ejecutar el flujo de usuario (p. ej. E2E con Playwright/Cypress) y verificar el resultado.
- **Automatización/ETL/n8n**: ejecutar el flujo (o un dry-run en una ventana acotada), verificar idempotencia,
  transaccionalidad y conteos; restaurar el estado de datos al terminar.
- **Datos/SQL**: validar el estado antes/después y la idempotencia de los statements.

Cubrir también los casos de error (entradas inválidas, recursos inexistentes, permisos) cuando apliquen.

### Step N+3 — Actualizar documentación técnica (OBLIGATORIO)
- Actualizar los `docs/*-standards.md`, modelo de datos, contratos/API o README afectados por el cambio.
- **Consistencia documental (OBLIGATORIO en cada cambio).** La documentación debe quedar **sincronizada con el cambio en el mismo PR** — un cambio está incompleto si deja docs desactualizados o contradictorios (base-standards §6):
  - Cada dato tiene **una fuente canónica**; los demás documentos **enlazan, no copian** (sin duplicación ni contradicciones).
  - Al **mover/renombrar/eliminar** un archivo, actualizar **todas** sus referencias (objetivo: 0 enlaces rotos).
  - Mantener al día el **índice/inventario** de documentación si se agrega, mueve o elimina un documento.

## 3. Plantilla de reporte (en `specs/<change-name>/reports/`)

```markdown
# Reporte Step N+1 — Pruebas y verificación de estado

- Fecha: AAAA-MM-DD
- Cambio: <change-name>
- Agente: <agent-name>

## Comandos ejecutados
- `<comando 1>`
- `<comando 2>`

## Resultados de pruebas
- Dirigidas: X pasaron, Y fallaron, Z omitidas
- Suite completa/requerida: X pasaron, Y fallaron, Z omitidas
- Duración: <duración>

## Verificación de estado
- Antes: <métrica/tabla/check>: <valor>
- Después: <métrica/tabla/check>: <valor>
- Estado restaurado: Sí/No — <acciones>

## Resultado
- Estado Step N+1: PASS/FAIL
- Bloqueos: <ninguno o lista>
```

## 4. Checklist de verificación antes de finalizar tasks.md

- [ ] Step 0 (crear feature branch) es el PRIMER paso
- [ ] Están todos los pasos obligatorios de config.yaml
- [ ] Pasos numerados secuencialmente y marcados "(OBLIGATORIO)"
- [ ] Step N+1 incluye ruta y nombre del reporte en `specs/<change-name>/reports/`
- [ ] Los pasos de verificación manual indican "EL AGENTE EJECUTA"
- [ ] Las tareas incluyen restauración de estado cuando mutan datos

## 5. Cuándo aplica

Al crear/actualizar `tasks.md` vía `/opsx:ff`, `/opsx:continue`, o al implementar con `/opsx:apply`.

## 6. Requisitos de ejecución del agente

**CRÍTICO**: al implementar tareas, el agente DEBE:
1. **Ejecutar todas las verificaciones manuales él mismo** (levantar servicios/entornos si hace falta).
2. **Marcar tareas como completadas (`[x]`) solo DESPUÉS** de ejecutar y verificar, restaurar estado y documentar.
3. **Nunca delegar las pruebas al usuario** ni marcar tareas sin ejecutarlas.
4. **Documentar** comandos ejecutados, resultados, restauración de estado e incidencias.

Si implementas tareas sin ejecutar tú mismo la verificación, estás violando esta regla.
