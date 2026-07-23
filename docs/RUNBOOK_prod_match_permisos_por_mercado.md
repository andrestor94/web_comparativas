# Mini-runbook — Deploy: permisos de Match independientes por mercado

**Qué cambia:** Match deja de tener una sola casilla (que habilitaba ambos mercados) y pasa a **dos claves independientes**: `mercado_privado.match` y `mercado_publico.match`. El módulo sigue siendo uno solo (misma vista, API y datos); solo cambia quién ve la entrada en cada sidebar y quién entra por cada ruta. **No toca los datos de Match ya pusheados** (corrida 1, 64.223 propuestas) ni nada pesado.

## 1. Deploy

```powershell
git push origin main
```

(Render redeploya solo. Sin variables nuevas ni pasos de datos.)

## 2. Qué buscar en el log de arranque de Render

- `[MIGRATION] Permisos Match por mercado: N usuario(s) migrados (clave vieja -> ambas claves). (OK, idempotente)` — N = usuarios que tenían la casilla vieja tildada (tu usuario incluido). En el próximo arranque va a decir `0 usuario(s)`: es lo esperado (idempotente).
- Que **NO** aparezca ninguna línea `[MIGRATION] ATENCION: module_access del user id=...` (sería un JSON ilegible en algún usuario — anotá el id y avisame).
- Que **NO** haya `InFailedSqlTransaction` ni `Traceback` en el bloque de migraciones.

## 3. Después del deploy — revisión de permisos (importante)

La migración les dejó **ambas** casillas tildadas a todos los que tenían la vieja (para no sacarle acceso a nadie). Entrá a **S.I.C. → Usuarios** y revisá usuario por usuario: la casilla **Match** ahora aparece **dos veces** (una bajo Mercado Privado, otra bajo Mercado Público) — **destildá la del mercado que no corresponda** en cada caso. Los usuarios sin configuración de módulos (legacy) siguen viendo Match en ambos mercados por su rol, como cualquier otro módulo.

## 4. Verificación rápida en la página real

Con un usuario de prueba (o editando el tuyo): tildar solo Privado → Match aparece solo en el sidebar de Mercado Privado y `/mercado-publico/match` rechaza; tildar solo Público → al revés; ambos → aparece en los dos con la misma pantalla y datos; ninguno → desaparece de ambos. La card de Dimensionamiento sigue en 256 · 158 · 98.

## Rollback

Render → Deploys → **Rollback** (o `git revert` de los 2 commits + push). La migración no necesita deshacerse: solo AGREGÓ la clave nueva a listas JSON; el código viejo la ignora sin efecto.
