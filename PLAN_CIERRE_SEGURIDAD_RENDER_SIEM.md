# Plan de Cierre de Seguridad — SIEM Suizo Argentina

**Clasificación:** Confidencial / Técnico  
**Sistema:** SIEM (web-comparativas-app)  
**Repositorio:** `https://github.com/andrestor94/web_comparativas.git`  
**Entorno productivo:** Render  
**Fecha:** 21 de mayo de 2026  
**Estado al momento de este documento:** Código corregido — pendientes operativos en Render, GitHub y Office 365.

---

## Contexto

Se realizaron dos rondas de correcciones de ciberseguridad sobre el proyecto SIEM. Los cambios en código están aplicados y verificados. Este documento cubre exclusivamente los pasos operativos que deben ejecutarse **fuera del código** para cerrar la validación de seguridad antes del deploy definitivo en Render.

---

## Resumen de estado actual

| Ítem | Estado |
|---|---|
| Contraseñas hardcodeadas en scripts | ✅ Corregido en código |
| Logs con DATABASE_URL | ✅ Corregido en código |
| APP_SECRET fallback inseguro | ✅ Corregido en código |
| Cookies de sesión inseguras | ✅ Corregido en código |
| Health check `/readyz` con validación DB | ✅ Implementado |
| Bootstrap admin sin hashing | ✅ Corregido en código |
| Fallback legacy contraseñas texto plano | ✅ Controlado por env var |
| DB_SSLMODE configurable | ✅ Implementado en código |
| `web_comparativas/.env` destrackeado de Git | ✅ Ejecutado (`git rm --cached`) |
| Credenciales SMTP en historial de Git | ⚠️ **Pendiente — acción urgente** |
| Purga del historial de Git | ⚠️ **Pendiente — acción urgente** |
| Variables de entorno configuradas en Render | ⚠️ **Pendiente — antes del deploy** |
| Backups confirmados en Render | ⚠️ **Pendiente de validación manual** |
| HTTPS y redirect HTTP → HTTPS | ⚠️ **Pendiente de validación manual** |
| `/healthz` y `/readyz` validados post-deploy | ⚠️ **Pendiente de ejecución** |

---

## Acciones urgentes antes de seguir

> **Estas acciones deben completarse ANTES de hacer deploy en Render y ANTES de notificar que el entorno está seguro.**

### 1.1 Rotación de contraseña SMTP

**Por qué es urgente:** El archivo `web_comparativas/.env` contenía credenciales SMTP reales y estuvo versionado en Git desde el primer commit (`c079ecdcb`). El repositorio es accesible en `github.com/andrestor94/web_comparativas`. Cualquier persona con acceso al repositorio —o que haya clonado el repo en cualquier momento desde noviembre de 2025— puede haber obtenido estas credenciales.

La contraseña debe considerarse **comprometida** hasta que sea rotada.

**Pasos:**

1. Ingresar al portal de administración de Microsoft 365:
   ```
   https://admin.microsoft.com
   ```
   o al portal de Azure Active Directory si se usa autenticación moderna.

2. Navegar a: **Usuarios → Usuario activo → Restablecimiento de contraseña** (o **Seguridad → App passwords** si se usa autenticación multifactor con contraseñas de aplicación).

3. Generar una nueva contraseña segura o app password para el uso SMTP.

4. **No** anotar la nueva contraseña en ningún archivo del proyecto ni en ningún chat.

5. Actualizar la nueva credencial **exclusivamente** en Render como variable de entorno:
   - Render Dashboard → Servicio web → Environment → `SMTP_PASS` → nuevo valor.

6. Actualizar el `.env` local si se necesita para desarrollo, pero **nunca subir ese archivo a Git**.

7. Verificar que el sistema puede enviar correos: ejecutar una acción que dispare un correo (por ejemplo, solicitud de restablecimiento de contraseña desde la UI) y confirmar llegada.

8. Guardar evidencia de la rotación: captura de pantalla del panel de Office 365 con la fecha de cambio de contraseña.

**Variables afectadas:**
- `SMTP_USER`: no fue expuesta como credencial pero está visible en el historial.
- `SMTP_PASS`: credencial comprometida — rotar obligatoriamente.

---

### 1.2 Purga de `web_comparativas/.env` del historial de Git

**Por qué:** Aunque el archivo ya fue destrackeado (`git rm --cached`), los commits anteriores siguen conteniendo el archivo con credenciales. Cualquier `git clone` del repositorio puede acceder al historial y recuperar el `.env` con `git show c079ecdcb:web_comparativas/.env`.

**Antes de empezar:**
- Hacer un backup completo del repositorio local.
- Avisar a todos los colaboradores que el historial será reescrito y deberán reclonar o sincronizar correctamente.
- Si el repositorio en GitHub es público, asumir que las credenciales ya están indexadas por bots. En ese caso, la rotación (paso 1.1) es la acción más importante.

**Opción A — `git filter-repo` (recomendada):**

```bash
# 1. Hacer backup del repo original
cp -r web_comparativas_v2-ok web_comparativas_backup_$(date +%Y%m%d)

# 2. Instalar git-filter-repo si no está disponible
pip install git-filter-repo

# 3. Clonar el repo como mirror limpio
git clone --mirror https://github.com/andrestor94/web_comparativas.git repo-clean.git
cd repo-clean.git

# 4. Eliminar el archivo del historial completo
git filter-repo --path web_comparativas/.env --invert-paths

# 5. Verificar que el archivo ya no aparece en ningún commit
git log --all --oneline -- web_comparativas/.env
# Resultado esperado: sin output

# 6. Forzar push al remote (DESTRUCTIVO — coordinar con el equipo)
git push --force --mirror

# 7. Volver al repo de trabajo y sincronizar
cd ..
git fetch --all
git reset --hard origin/main
```

**Opción B — BFG Repo Cleaner:**

```bash
# 1. Descargar BFG desde: https://rtyley.github.io/bfg-repo-cleaner/
# (archivo .jar — requiere Java instalado)

# 2. Clonar el repo como mirror
git clone --mirror https://github.com/andrestor94/web_comparativas.git repo-clean.git

# 3. Eliminar el archivo del historial
java -jar bfg.jar --delete-files .env repo-clean.git

# 4. Limpiar y comprimir el historial
cd repo-clean.git
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# 5. Forzar push
git push --force --mirror
```

**Después de la purga:**
- Todos los colaboradores deben eliminar su copia local y reclonar desde cero.
- Verificar en GitHub que el commit `c079ecdcb` ya no contiene el archivo:
  ```
  https://github.com/andrestor94/web_comparativas/blob/c079ecdcb/web_comparativas/.env
  ```
  Debe retornar 404.
- Si el repositorio fue clonado por herramientas de CI/CD externas, actualizar esas copias también.

---

## Variables de entorno obligatorias en Render

Configurar en: **Render Dashboard → Servicio web → Environment**

> Los valores de ejemplo son referenciales. **Nunca copiar estos valores directamente** — generar los secretos con los comandos indicados.

| Variable | Obligatoria en prod | Valor esperado / formato | Observación |
|---|---|---|---|
| `APP_ENV` | **Sí** | `production` | Activa: APP_SECRET obligatoria, https_only=True, validaciones de arranque. Sin esto, la app corre en modo desarrollo. |
| `APP_SECRET` | **Sí** | Token hex ≥ 32 chars | Generar con: `python -c "import secrets; print(secrets.token_hex(32))"`. Único por entorno, nunca reutilizar. |
| `DATABASE_URL` | **Sí** | `postgresql://user:pass@host/db` | Usar **Internal Database URL** de Render (host tipo `dpg-xxxxxx-a`). Render la inyecta automáticamente si la DB está en la misma cuenta. |
| `DB_SSLMODE` | **Sí** | `require` | Fuerza cifrado TLS en la conexión a PostgreSQL. Default del código en producción si no se define: `require`. Configurar explícitamente para evidencia. |
| `SMTP_HOST` | Sí (si se usa correo) | `smtp.office365.com` | Servidor SMTP corporativo. |
| `SMTP_PORT` | Sí (si se usa correo) | `587` | Puerto STARTTLS. |
| `SMTP_USER` | Sí (si se usa correo) | `usuario@suizoargentina.com` | Cuenta SMTP corporativa. Configurar solo en Render, nunca en archivos del repo. |
| `SMTP_PASS` | Sí (si se usa correo) | Contraseña rotada (ver paso 1.1) | **Usar la nueva contraseña después de la rotación**. Nunca la credencial expuesta. |
| `SMTP_TLS` | Sí (si se usa correo) | `1` | Habilita STARTTLS. |
| `ADMIN_EMAILS` | Depende | `admin@suizoargentina.com` | Lista de admins que reciben notificaciones. |
| `APP_PUBLIC_URL` | Depende | `https://tu-app.onrender.com` | URL pública del servicio en Render. |
| `ALLOW_LEGACY_PLAINTEXT_PASSWORDS` | No | `false` (o no definir) | No definir en producción. Solo `true` durante migración controlada de usuarios legacy. |
| `ADMIN_EMAIL` | Solo bootstrap | `admin@empresa.com` | Solo en el primer arranque para crear el usuario admin inicial. **Eliminar después del primer deploy exitoso.** |
| `ADMIN_PASSWORD` | Solo bootstrap | Contraseña segura ≥ 12 chars | Solo en el primer arranque. **Eliminar después del primer deploy exitoso.** Generar con: `python -c "import secrets; print(secrets.token_urlsafe(18))"` |

**Cómo generar APP_SECRET en Windows:**
```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

**Cómo generar una contraseña de admin inicial:**
```powershell
python -c "import secrets; print(secrets.token_urlsafe(18))"
```

---

## Checklist Render Dashboard

Ejecutar en: **https://dashboard.render.com**

### Variables de entorno del servicio web

- [ ] `APP_ENV` = `production` configurado en Variables del servicio web.
- [ ] `APP_SECRET` definido con valor aleatorio ≥ 32 caracteres. Verificar longitud sin revelar el valor.
- [ ] `DATABASE_URL` configurado. Verificar que el host **no contiene `.render.com`** (Internal URL) sino un identificador interno tipo `dpg-xxxxxx-a`.
- [ ] `DB_SSLMODE` = `require` configurado.
- [ ] Variables SMTP configuradas con la nueva contraseña rotada.
- [ ] `ADMIN_EMAIL` y `ADMIN_PASSWORD` **eliminados** después del primer deploy exitoso.
- [ ] `ALLOW_LEGACY_PLAINTEXT_PASSWORDS` **no definida** (o `false`).

### HTTPS y red

- [ ] El servicio web tiene un dominio con certificado SSL activo (visible en Render Dashboard → servicio → Settings).
- [ ] La opción **"Redirect HTTP to HTTPS"** está habilitada (Render Dashboard → servicio → Settings → HTTP to HTTPS).
- [ ] El dominio responde en `https://` sin advertencias de certificado.

### Base de datos

- [ ] La base de datos PostgreSQL aparece en la misma cuenta de Render (Render Dashboard → Databases).
- [ ] El servicio web usa la **Internal Database URL** (verificar en la variable `DATABASE_URL` del servicio).
- [ ] El puerto 5432 de la base de datos **no está abierto públicamente** (si se usa Internal URL, esto es automático).
- [ ] La sección **Connectivity** de la DB de Render confirma que SSL está requerido o disponible.

### Backups

- [ ] Render Dashboard → PostgreSQL → pestaña **Backups**: backups automáticos activos.
- [ ] El plan de la base de datos **no es Free Tier** (el plan gratuito no tiene backups y borra la DB a los 90 días).
- [ ] La retención de backups es de al menos **7 días**.
- [ ] Se realizó al menos un **simulacro de restauración** en staging para verificar que los backups son funcionales.

### Logs y monitoreo

- [ ] Render Dashboard → servicio → **Logs**: el servicio arrancó sin errores de configuración (no debe aparecer `CONFIGURACIÓN INVÁLIDA` ni `RuntimeError`).
- [ ] Búsqueda en los logs por `postgresql://`, `password`, `SMTP_PASS`, `DATABASE_URL`: **sin resultados con credenciales en texto claro**.
- [ ] Búsqueda en los logs por `APP_SECRET`: no debe aparecer el valor, solo mensajes de validación.

---

## Validaciones post-deploy

Ejecutar después de que el servicio esté levantado en Render.

### Comandos de validación

**Reemplazar `<DOMINIO>` con la URL real del servicio en Render.**

```bash
# 1. Liveness check (la app está viva)
curl -i https://<DOMINIO>/healthz
```
Resultado esperado:
```
HTTP/2 200
{"status":"ok"}
```

```bash
# 2. Readiness check (la app puede conectarse a la DB)
curl -i https://<DOMINIO>/readyz
```
Resultado esperado:
```
HTTP/2 200
{"status":"ok","database":"ok"}
```
Si retorna `503`:
```json
{"status":"degraded","database":"error"}
```
El problema es de conectividad a la base de datos — verificar `DATABASE_URL` y `DB_SSLMODE` en Render.

```bash
# 3. Verificar que HTTP redirige a HTTPS
curl -i http://<DOMINIO>/healthz
```
Resultado esperado:
```
HTTP/1.1 301 Moved Permanently
Location: https://<DOMINIO>/healthz
```

### Pruebas manuales desde el navegador

Ejecutar desde una ventana de incógnito para evitar sesiones previas:

| Prueba | Pasos | Resultado esperado |
|---|---|---|
| Login | Ir a `/login`, ingresar credenciales del admin | Redirige al dashboard, sesión activa |
| Endpoint protegido sin sesión | Ir a `/sic` sin sesión activa | Redirige a login (HTTP 401 o 302) |
| Módulo principal | Navegar por el dashboard, listado de comparativas | Datos cargados desde PostgreSQL |
| Crear usuario | Ir a `/sic/users/new`, ingresar email y contraseña ≥ 12 chars | Usuario creado sin errores |
| Crear usuario sin contraseña | Dejar contraseña vacía o < 12 chars | Error `password_minimo_12` — no crea usuario |
| Logout | Hacer click en cerrar sesión | Sesión terminada, redirige a login |
| Envío SMTP (si aplica) | Solicitar restablecimiento de contraseña | Correo llega a la bandeja de entrada |
| Revisión de logs | Render Dashboard → Logs mientras se ejecutan las pruebas | Sin contraseñas, URLs de DB ni tokens visibles |

---

## Validación de limpieza de Git

### Verificar que `.env` ya no está trackeado

```bash
# No debe retornar ninguna línea con ".env" como archivo trackeado
git ls-files | grep -i "\.env"
# Resultado esperado: solo .env.example (archivo sin credenciales reales)

# Confirmar que .gitignore cubre web_comparativas/.env
git check-ignore -v web_comparativas/.env
# Resultado esperado: .gitignore:24:.env    web_comparativas/.env
```

### Verificar secretos residuales en archivos actuales

```bash
# Buscar patrones sensibles en todos los archivos Python
grep -RniE "admin123|TuClaveFuerte123|dev-secret-123|password123|https_only=False" \
  --include="*.py" \
  --exclude-dir=".git" \
  --exclude-dir="venv_webcomparativas" \
  --exclude-dir="__pycache__" \
  .
```
Resultado esperado: Sin coincidencias en código activo. Las únicas coincidencias aceptables son en comentarios de `legacy_routes.py`.

```bash
# Buscar DATABASE_URL impresa en logs
grep -RniE "print.*DATABASE_URL|log.*DATABASE_URL" \
  --include="*.py" \
  --exclude-dir=".git" \
  --exclude-dir="venv_webcomparativas" \
  .
```
Resultado esperado: Sin coincidencias.

```bash
# Buscar la contraseña SMTP expuesta (verificar que no está en ningún archivo Python ni Markdown activo)
grep -RniE "<CONTRASEÑA_SMTP_ROTADA>" \
  --exclude-dir=".git" \
  --exclude-dir="venv_webcomparativas" \
  .
```
Resultado esperado: Sin coincidencias. Si aparece en algún `.md` de documentación interna, eliminar esa referencia.

### Verificar que el historial de Git fue purgado (después de ejecutar paso 1.2)

```bash
# El commit donde estaba el .env es c079ecdcb
# Verificar que ya no contiene el archivo
git show c079ecdcb:web_comparativas/.env
# Resultado esperado después de la purga: error "path 'web_comparativas/.env' does not exist in 'c079ecdcb'"

# Verificar que ningún commit contiene el archivo
git log --all --oneline -- web_comparativas/.env
# Resultado esperado después de la purga: sin output
```

---

## Evidencias recomendadas para auditoría interna

Guardar en una carpeta segura (no en el repositorio Git):

| Evidencia | Cómo obtenerla | Qué debe mostrar |
|---|---|---|
| Variables de entorno en Render | Captura de pantalla del panel de Environment de Render | Lista de variables configuradas, **sin mostrar los valores** (Render los oculta por defecto con `***`) |
| Backups activos | Captura de Render → PostgreSQL → Backups | Backups listados con fecha y estado |
| HTTPS activo | Captura del browser en `https://<DOMINIO>` mostrando el candado | Certificado válido, sin advertencias |
| Redirect HTTP → HTTPS | Resultado de `curl -i http://<DOMINIO>/healthz` | Status 301 con Location hacia `https://` |
| Resultado `/healthz` | `curl -i https://<DOMINIO>/healthz` | `{"status":"ok"}` con HTTP 200 |
| Resultado `/readyz` | `curl -i https://<DOMINIO>/readyz` | `{"status":"ok","database":"ok"}` con HTTP 200 |
| Log de deploy sin secretos | Captura de Render → Logs durante el arranque | Mensajes de startup sin DATABASE_URL, sin passwords, sin APP_SECRET |
| Rotación SMTP | Captura del panel de Office 365 con fecha de cambio de contraseña | Fecha posterior al 21 de mayo de 2026 |
| `.env` destrackeado | Resultado de `git ls-files` | Solo `.env.example` — sin `.env` real |
| Purga de historial | Resultado de `git log --all --oneline -- web_comparativas/.env` | Sin output |
| Checklist firmado | Este documento, con cada ítem marcado y fecha de validación | — |

---

## Criterio final de aprobación

### Condiciones para "Apto para producción"

El sistema puede considerarse **apto para producción** cuando se cumplan **todos** los siguientes puntos:

**Código (ya completado):**
- [x] Sin contraseñas hardcodeadas en scripts.
- [x] Sin exposición de `DATABASE_URL` en logs.
- [x] `APP_SECRET` obligatoria en producción con validación de longitud.
- [x] `https_only=True` en producción.
- [x] `/readyz` valida conectividad con la base de datos.
- [x] Bootstrap admin hashea la contraseña antes de persistirla.
- [x] Fallback legacy de contraseñas deshabilitado por defecto.
- [x] `DB_SSLMODE` configurable por variable de entorno.
- [x] `web_comparativas/.env` destrackeado de Git.

**Pendientes operativos (deben completarse antes del deploy):**
- [ ] Contraseña SMTP rotada en Office 365.
- [ ] Nueva contraseña SMTP configurada en Render (no la expuesta).
- [ ] `APP_ENV=production` en Render.
- [ ] `APP_SECRET` ≥ 32 chars en Render.
- [ ] `DATABASE_URL` usa Internal Database URL en Render.
- [ ] `DB_SSLMODE=require` en Render.
- [ ] HTTPS activo con redirect HTTP → HTTPS.
- [ ] Backups automáticos confirmados activos.
- [ ] Plan de DB no es Free Tier.
- [ ] `/healthz` responde `{"status":"ok"}`.
- [ ] `/readyz` responde `{"status":"ok","database":"ok"}`.
- [ ] Logs de Render sin credenciales.

**Pendiente de largo plazo (no bloquea el deploy pero debe completarse):**
- [ ] Historial de Git purgado del `.env` con credenciales.

---

### Clasificación de estado

| Estado | Descripción |
|---|---|
| ✅ **Apto para producción** | Todos los puntos del código completados + todos los pendientes operativos completados |
| ⚠️ **Apto con pendientes menores** | Código OK + pendientes operativos completados + solo queda la purga del historial de Git |
| 🔴 **No apto para producción** | Contraseña SMTP no rotada, o `APP_ENV` no configurado, o `APP_SECRET` ausente, o `/readyz` falla |

**Estado actual de este proyecto:** 🔴 **No apto para producción** — pendiente rotación de SMTP y configuración de variables en Render.

---

## Orden de ejecución recomendado

```
1. Rotar contraseña SMTP en Office 365              [URGENTE — antes de todo]
2. Purgar historial de Git (opcional pero recomendado) [requiere coordinación]
3. Hacer commit de todos los cambios de código       [git commit]
4. Configurar variables de entorno en Render
5. Hacer deploy en Render
6. Validar /healthz y /readyz
7. Ejecutar pruebas manuales de login / módulos
8. Revisar logs de Render
9. Eliminar ADMIN_EMAIL y ADMIN_PASSWORD de Render
10. Guardar evidencias
11. Actualizar este checklist con fecha de validación
```

---

*Documento generado como parte del cierre de seguridad del ciclo de reparaciones de ciberseguridad del SIEM Suizo Argentina — mayo 2026.*
