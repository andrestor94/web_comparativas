# Cierre operativo de seguridad SIEM - Render

**Sistema:** SIEM — web-comparativas-app  
**Repositorio:** `https://github.com/andrestor94/web_comparativas.git`  
**Responsable técnico:** Andrés Torres  
**Fecha:** 21 de mayo de 2026  
**Clasificación:** Confidencial / Procedimiento operativo

---

## Estado actual

| Área | Estado |
|---|---|
| Correcciones de código | ✅ Ejecutado — 11 archivos modificados, sintaxis verificada |
| `web_comparativas/.env` destrackeado de Git | ✅ Ejecutado — `git rm --cached` aplicado, pendiente commit |
| Commit de cierre con todos los cambios | ⚠️ **Pendiente** |
| Rotación de credencial SMTP | 🔴 **Pendiente — acción urgente** |
| Purga del historial de Git | 🔴 **Pendiente — acción urgente** |
| Variables de entorno configuradas en Render | 🔴 **Pendiente** |
| Deploy en Render | 🔴 **Pendiente** |
| Validaciones post-deploy | 🔴 **Pendiente** |
| Backups confirmados | ⚠️ **Requiere validación manual en Render** |
| HTTPS y redirect HTTP → HTTPS | ⚠️ **Requiere validación manual en Render** |
| Evidencias guardadas | 🔴 **Pendiente** |

**Clasificación actual:** 🔴 No apto para producción

---

## Orden exacto de ejecución

Seguir este orden sin saltear pasos. Cada paso depende del anterior.

```
Paso 0 → Commit de cierre (todos los cambios de código al historial)
Paso 1 → Rotar credencial SMTP (URGENTE — hacerlo ANTES de continuar)
Paso 2 → Purgar historial de Git
Paso 3 → Configurar variables en Render
Paso 4 → Deploy controlado
Paso 5 → Validaciones post-deploy
Paso 6 → Guardar evidencias de cierre
```

---

## Paso 0 — Commit de cierre de seguridad

**Estado:** ⚠️ Pendiente  
**Ejecutar en:** Terminal local, directorio del proyecto

Todos los cambios de código están aplicados pero no commiteados. Antes de continuar, registrarlos en Git.

```bash
cd "web_comparativas_v2- ok"

# Verificar qué archivos están pendientes de commit
git status

# Agregar los archivos modificados (uno a uno para evitar incluir archivos no deseados)
git add web_comparativas/main.py
git add web_comparativas/auth.py
git add web_comparativas/models.py
git add web_comparativas/routers/sic_router.py
git add web_comparativas/seed_admin.py
git add web_comparativas/reset_admin_password.py
git add reset_admin.py
git add reset_admin_simple.py
git add seed_admin.py
git add add_access_scope_column.py
git add migrate_local_to_render.py
git add SECURITY_RENDER.md
git add PLAN_CIERRE_SEGURIDAD_RENDER_SIEM.md
git add CIERRE_OPERATIVO_SEGURIDAD_SIEM.md

# El .env ya fue destrackeado — confirmar que está en el staging como eliminado
git status web_comparativas/.env
# Debe mostrar: D  web_comparativas/.env

# Crear el commit de cierre
git commit -m "security: eliminar credenciales hardcodeadas y reforzar configuracion de produccion

- Scripts de admin/seed/reset usan getpass() o variable de entorno
- APP_SECRET obligatoria en produccion con validacion de longitud minima
- https_only=True en produccion (controlado por APP_ENV)
- /readyz valida conectividad con PostgreSQL
- Bootstrap admin hashea password antes de persistir
- ALLOW_LEGACY_PLAINTEXT_PASSWORDS controla fallback legacy
- DB_SSLMODE configurable por variable de entorno
- Logs de DATABASE_URL enmascarados (solo host y nombre de base)
- web_comparativas/.env destrackeado del repositorio
- Documentacion operativa de seguridad para Render"

# Verificar el commit
git log --oneline -3
```

**Validación:** El comando `git status` debe mostrar "nothing to commit, working tree clean".

> ⚠️ **No hacer push todavía.** El push se hace en el Paso 2, después de purgar el historial, para evitar que las credenciales queden en GitHub entre el commit actual y la purga.

---

## Paso 1 — Rotar credencial SMTP

**Estado:** 🔴 Pendiente  
**Ejecutar en:** Portal Office 365 / Microsoft Entra ID  
**Puede delegarse a:** IT / Administrador de Microsoft 365

### Por qué es urgente

El archivo `web_comparativas/.env` contenía credenciales de la cuenta de correo corporativa. Ese archivo estuvo versionado en Git desde el primer commit del proyecto (noviembre de 2025) y el repositorio está alojado en GitHub (`github.com/andrestor94/web_comparativas`).

Cualquier persona que haya clonado el repositorio en cualquier momento puede recuperar esas credenciales con:

```bash
git show c079ecdcb:web_comparativas/.env
```

**La credencial debe considerarse comprometida hasta que sea rotada.**

### Cuenta afectada

- **Cuenta:** La cuenta de correo corporativa configurada como `SMTP_USER` en el archivo `.env`.
- **No repetir el valor aquí.** El valor puede verse en el historial de Git con el comando anterior, pero no debe propagarse más.

### Pasos de rotación

**1. Acceder al panel de administración:**

```
https://admin.microsoft.com
  → Usuarios → Usuarios activos → [usuario afectado]
  → Restablecer contraseña
```

Si la cuenta usa **autenticación multifactor (MFA)** y el SMTP se configura con una **App Password**:

```
https://mysignins.microsoft.com/security-info
  → Contraseñas de aplicación → Eliminar la existente → Crear nueva
```

**2. Generar la nueva credencial:**
- Debe ser una contraseña fuerte (mínimo 16 caracteres, combinación de mayúsculas, minúsculas, números y símbolos).
- Anotar el nuevo valor solo en un gestor de contraseñas seguro (Bitwarden, 1Password, KeePass). No en archivos de texto, chats ni correos.

**3. Actualizar en Render:**

```
Render Dashboard
  → web-comparativas-app (servicio web)
  → Environment
  → SMTP_PASS → editar → pegar el nuevo valor
  → Save Changes
```

**4. Verificar envío de correo:**
- Desde la aplicación en Render, ejecutar una acción que dispare un correo (por ejemplo, solicitud de restablecimiento de contraseña).
- Confirmar llegada a la bandeja de destino.
- Verificar que los logs de Render no muestran errores de autenticación SMTP.

**5. Evidencia a guardar:**
- Captura de pantalla del panel de Microsoft 365 mostrando la fecha del último cambio de contraseña (sin mostrar el valor).
- O ticket de IT con número de solicitud y fecha de ejecución.

---

## Paso 2 — Purgar historial de Git

**Estado:** 🔴 Pendiente  
**Ejecutar en:** Terminal con acceso al repositorio  
**Requiere coordinación con:** Todos los colaboradores del repositorio

### Advertencia previa

> ⚠️ **Esta operación reescribe el historial de Git.** Es destructiva e irreversible. Todos los colaboradores que tengan el repositorio clonado deberán sincronizar o reclonar después del push forzado. Coordinar antes de ejecutar.

> ⚠️ **Hacer backup completo antes de empezar.**

### Contexto

El archivo con credenciales entró en el commit `c079ecdcb` ("Primera versión del proyecto"). Está en el historial desde el inicio y debe eliminarse de todos los commits que lo contienen.

### Método recomendado — `git filter-repo`

```bash
# 0. Hacer backup del directorio local
cp -r "web_comparativas_v2- ok" "web_comparativas_backup_$(Get-Date -Format 'yyyyMMdd')"

# 1. Instalar git-filter-repo (requiere Python)
pip install git-filter-repo

# 2. Clonar el repositorio como mirror en un directorio limpio
#    (NO trabajar sobre el clone local existente)
git clone --mirror https://github.com/andrestor94/web_comparativas.git siem-clean.git

# 3. Entrar al mirror
cd siem-clean.git

# 4. Eliminar web_comparativas/.env de todo el historial
git filter-repo --path web_comparativas/.env --invert-paths

# 5. Verificar que el archivo ya no aparece en ningún commit
git log --all --oneline -- web_comparativas/.env
# Resultado esperado: sin output (ninguna línea)

# 6. Forzar push al repositorio remoto
#    (DESTRUCTIVO — sobreescribe el historial de GitHub)
git push --force --mirror

# 7. Volver al directorio de trabajo y sincronizar
cd ..
git fetch --all
git reset --hard origin/main
```

### Método alternativo — BFG Repo Cleaner

```bash
# Requiere Java instalado
# Descargar BFG desde: https://rtyley.github.io/bfg-repo-cleaner/

# 1. Clonar como mirror
git clone --mirror https://github.com/andrestor94/web_comparativas.git siem-clean.git

# 2. Ejecutar BFG para eliminar el archivo
java -jar bfg.jar --delete-files .env siem-clean.git

# 3. Limpiar y compactar
cd siem-clean.git
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# 4. Forzar push
git push --force --mirror
```

### Verificación post-purga

```bash
# Verificar que el commit original ya no contiene el .env
git show c079ecdcb:web_comparativas/.env
# Resultado esperado: fatal: Path 'web_comparativas/.env' does not exist in 'c079ecdcb'

# Verificar que ningún commit en el historial contiene el archivo
git log --all --oneline -- web_comparativas/.env
# Resultado esperado: sin output

# Verificar en GitHub (reemplazar <HASH> con el SHA del primer commit)
# Abrir en browser: https://github.com/andrestor94/web_comparativas/blob/c079ecdcb/web_comparativas/.env
# Resultado esperado: página 404
```

### Comunicación a colaboradores

Después del push forzado, todos los colaboradores deben ejecutar:

```bash
# Opción 1: sincronizar el repositorio existente
git fetch --all
git reset --hard origin/main

# Opción 2 (más limpia): eliminar el clon local y reclonar desde cero
cd ..
rm -rf web_comparativas_v2-ok
git clone https://github.com/andrestor94/web_comparativas.git web_comparativas_v2-ok
```

> **Nota:** Si el repositorio en GitHub es o fue público en algún momento, asumir que bots de indexación pueden haber capturado las credenciales. La rotación del paso 1 sigue siendo la acción más importante.

---

## Paso 3 — Configurar variables en Render

**Estado:** 🔴 Pendiente  
**Ejecutar en:** Render Dashboard — https://dashboard.render.com  
**Servicio:** web-comparativas-app

### Tabla de variables de entorno

| Variable | Valor esperado | Acción |
|---|---|---|
| `APP_ENV` | `production` | Crear o actualizar → valor: `production` |
| `APP_SECRET` | Token hex aleatorio ≥ 32 chars | Generar con el comando de abajo → pegar en Render |
| `DATABASE_URL` | `postgresql://user:pass@dpg-xxxxx/db` | Verificar que usa **Internal URL** (host sin `.render.com`) |
| `DB_SSLMODE` | `require` | Crear → valor: `require` |
| `SMTP_HOST` | `smtp.office365.com` | Verificar o actualizar |
| `SMTP_PORT` | `587` | Verificar o actualizar |
| `SMTP_USER` | Cuenta SMTP corporativa | Verificar que coincide con la cuenta rotada |
| `SMTP_PASS` | **Nueva contraseña del paso 1** | Actualizar con el valor rotado |
| `SMTP_TLS` | `1` | Crear si no existe |
| `ALLOW_LEGACY_PLAINTEXT_PASSWORDS` | `false` | Crear → valor: `false` (o no definir) |
| `ADMIN_EMAIL` | `admin@suizoargentina.com` | Solo si es el primer deploy — **eliminar después** |
| `ADMIN_PASSWORD` | Contraseña temporal generada | Solo si es el primer deploy — **eliminar después** |

### Generar APP_SECRET

Ejecutar en terminal local:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

Ejemplo de formato (no usar este valor):
```
a3f8b2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1
```

Copiar el resultado directamente al campo `APP_SECRET` en Render. No guardarlo en ningún archivo del proyecto.

### Generar contraseña temporal de admin (solo bootstrap)

```bash
python -c "import secrets; print(secrets.token_urlsafe(18))"
```

Usar este valor como `ADMIN_PASSWORD` solo para el primer arranque. Eliminarlo de Render después.

### Verificar Internal Database URL

En Render Dashboard:
```
Servicio web → Environment → DATABASE_URL
```

El host en la URL debe tener formato `dpg-xxxxxxxxxx-a` (sin `.render.com`). Si contiene `.render.com` es la External URL — solicitar a Render la Internal URL desde:
```
Render Dashboard → Databases → [nombre de la DB] → Connect → Internal Database URL
```

### Checklist de variables antes del deploy

- [ ] `APP_ENV=production` configurado
- [ ] `APP_SECRET` de al menos 32 caracteres configurado
- [ ] `DATABASE_URL` usa Internal URL (host sin `.render.com`)
- [ ] `DB_SSLMODE=require` configurado
- [ ] `SMTP_PASS` actualizado con la nueva contraseña rotada
- [ ] `ALLOW_LEGACY_PLAINTEXT_PASSWORDS=false` configurado (o no definido)

---

## Paso 4 — Deploy controlado

**Estado:** 🔴 Pendiente

### Secuencia de deploy

**1. Confirmar variables antes de deployar:**
- Revisar la lista del Paso 3. No hacer deploy si falta `APP_SECRET` o `DATABASE_URL`.

**2. Ejecutar el deploy:**
```
Render Dashboard → web-comparativas-app → Manual Deploy → Deploy latest commit
```
O hacer push a la rama principal si Render está configurado para auto-deploy:
```bash
git push origin main
```

**3. Monitorear los logs de arranque:**
```
Render Dashboard → web-comparativas-app → Logs
```

### Mensajes esperados en logs (arranque exitoso)

```
[DB] Connection type=INTERNAL host=dpg-xxxxxx sslmode=require pool_recycle=600s
[STARTUP] Lifespan startup begin
[STARTUP][DB] Backend: postgresql | Host: dpg-xxxxxx | Database: <nombre>
[STARTUP][DB] Motor: PostgreSQL (Render) ✓
[MIGRATION] SUCCESS: 'access_scope' checked/added.
```

### Errores esperados si falta alguna variable

| Variable faltante o incorrecta | Error en logs |
|---|---|
| `APP_SECRET` no definida en producción | `RuntimeError: CONFIGURACIÓN INVÁLIDA: APP_SECRET es obligatoria en producción` |
| `APP_SECRET` con menos de 32 chars | `RuntimeError: CONFIGURACIÓN INVÁLIDA: APP_SECRET debe tener al menos 32 caracteres` |
| `DATABASE_URL` incorrecta o inaccesible | `OperationalError: could not connect to server` o error de timeout en startup |
| `DATABASE_URL` apunta a Render pero `APP_ENV` no es `production` | `RuntimeError: BLOQUEO DE SEGURIDAD — entorno local apuntando a producción` |

Si aparece `RuntimeError` en los logs, el servicio no se levanta. Verificar la variable indicada en el mensaje, corregirla en Render, y hacer redeploy.

**4. Verificar que no aparecen secretos en logs:**

Buscar en los logs de arranque (Render → Logs):
- `postgresql://` → no debe aparecer
- `password` → solo puede aparecer en mensajes de migración de columnas, nunca como valor
- `APP_SECRET` → solo puede aparecer en un mensaje de advertencia si no está definida (nunca el valor)
- `SMTP_PASS` → no debe aparecer

**5. Si se usaron `ADMIN_EMAIL` y `ADMIN_PASSWORD` para bootstrap:**

Después de que el servicio arranque correctamente y el usuario admin pueda iniciar sesión, eliminar esas variables:
```
Render Dashboard → Environment → ADMIN_EMAIL → eliminar
Render Dashboard → Environment → ADMIN_PASSWORD → eliminar
→ Save Changes → Redeploy automático (o hacer deploy manual)
```

---

## Paso 5 — Validaciones post-deploy

**Estado:** 🔴 Pendiente  
**Reemplazar `<DOMINIO>` con la URL real del servicio.**

### Comandos de validación

**Liveness check:**
```bash
curl -i https://<DOMINIO>/healthz
```
Resultado esperado:
```
HTTP/2 200
content-type: application/json

{"status":"ok"}
```

**Readiness check (valida conectividad con PostgreSQL):**
```bash
curl -i https://<DOMINIO>/readyz
```
Resultado esperado:
```
HTTP/2 200
content-type: application/json

{"status":"ok","database":"ok"}
```

Si retorna `503`:
```json
{"status":"degraded","database":"error"}
```
→ Problema de conectividad con la base de datos. Verificar `DATABASE_URL` y `DB_SSLMODE` en Render.

**Verificar redirect HTTP → HTTPS:**
```bash
curl -i http://<DOMINIO>/healthz
```
Resultado esperado:
```
HTTP/1.1 301 Moved Permanently
Location: https://<DOMINIO>/healthz
```
Si retorna `200` sin redirect → habilitar la opción en Render Dashboard → Settings → Redirect HTTP to HTTPS.

### Checklist de pruebas manuales

Ejecutar desde una ventana de incógnito (sin sesión previa):

| # | Prueba | Pasos | Resultado esperado | Estado |
|---|---|---|---|---|
| 1 | Login | Ir a `https://<DOMINIO>/login`, ingresar credenciales del admin | Redirige al dashboard, sesión activa | ⚠️ Pendiente |
| 2 | Logout | Click en cerrar sesión desde el dashboard | Sesión terminada, redirige al login | ⚠️ Pendiente |
| 3 | Ruta protegida sin sesión | Ir a `https://<DOMINIO>/sic` sin estar logueado | Redirige a login (HTTP 401 o 302) | ⚠️ Pendiente |
| 4 | Módulo principal | Navegar por el dashboard de comparativas | Datos cargados, sin errores 500 | ⚠️ Pendiente |
| 5 | Lectura desde PostgreSQL | Consultar cualquier listado que requiera datos de la DB | Datos visibles, no vacíos | ⚠️ Pendiente |
| 6 | Crear usuario (contraseña válida) | `/sic/users/new`, email + contraseña ≥ 12 chars | Usuario creado exitosamente | ⚠️ Pendiente |
| 7 | Crear usuario (contraseña corta) | `/sic/users/new`, dejar contraseña vacía o < 12 chars | Error `password_minimo_12`, no crea usuario | ⚠️ Pendiente |
| 8 | Envío SMTP | Solicitar restablecimiento de contraseña desde el login | Correo llega a la bandeja de entrada | ⚠️ Pendiente |
| 9 | Logs sin secretos | Render → Logs durante todas las pruebas anteriores | Sin passwords, URLs de DB ni tokens visibles | ⚠️ Pendiente |
| 10 | HTTPS activo | Abrir `https://<DOMINIO>` en browser | Candado verde, sin advertencias de certificado | ⚠️ Pendiente |
| 11 | Redirect HTTP → HTTPS | Abrir `http://<DOMINIO>` en browser | Redirige automáticamente a `https://` | ⚠️ Pendiente |
| 12 | Backups activos | Render Dashboard → Databases → Backups | Lista de backups con fecha reciente | ⚠️ Pendiente |

### Checklist de seguridad en Render Dashboard

- [ ] `APP_ENV=production` visible en Environment (valor oculto por Render con `***`)
- [ ] `APP_SECRET` presente y oculto
- [ ] `DATABASE_URL` usa host interno (sin `.render.com`)
- [ ] `DB_SSLMODE=require` visible
- [ ] `SMTP_PASS` actualizado (variable presente)
- [ ] `ADMIN_EMAIL` y `ADMIN_PASSWORD` **eliminados** del entorno
- [ ] Backups automáticos activos en pestaña Backups
- [ ] Plan de DB no es Free Tier
- [ ] HTTPS activo con certificado válido
- [ ] Redirect HTTP → HTTPS habilitado

---

## Paso 6 — Evidencias de cierre

**Estado:** 🔴 Pendiente  
**Guardar en:** Carpeta segura interna, fuera del repositorio Git.

| # | Evidencia | Cómo obtenerla | Qué debe mostrar |
|---|---|---|---|
| 1 | Rotación SMTP | Captura del panel de Office 365 o ticket de IT | Fecha de cambio de contraseña posterior al 21/05/2026 |
| 2 | Variables en Render | Captura de Render → Environment | Lista de variables con valores ocultos (`***`) — sin mostrar secretos |
| 3 | Backups activos | Captura de Render → Databases → Backups | Backups listados con fecha y estado "Completed" |
| 4 | `/healthz` OK | `curl -i https://<DOMINIO>/healthz` | `HTTP/2 200` + `{"status":"ok"}` |
| 5 | `/readyz` OK | `curl -i https://<DOMINIO>/readyz` | `HTTP/2 200` + `{"status":"ok","database":"ok"}` |
| 6 | Logs de deploy limpios | Captura de Render → Logs durante el arranque | Sin passwords, URLs de conexión ni tokens |
| 7 | `.env` no trackeado | `git ls-files \| grep ".env"` | Solo `.env.example` — sin `.env` real |
| 8 | Purga de historial | `git log --all --oneline -- web_comparativas/.env` | Sin output (ningún commit) |
| 9 | Commit de cierre | `git log --oneline -3` | Commit de seguridad visible en el historial |
| 10 | Checklist firmado | Este documento con todos los ítems completados | Fecha de cierre y firma del responsable |

---

## Criterio final de aprobación

| Condición | Estado requerido |
|---|---|
| Credencial SMTP rotada en Office 365 | ✅ Ejecutado |
| Nueva contraseña SMTP actualizada en Render | ✅ Ejecutado |
| `web_comparativas/.env` destrackeado de Git | ✅ Ejecutado |
| Historial de Git purgado del `.env` | ✅ Ejecutado (o aceptado como riesgo residual documentado) |
| Commit de cierre de seguridad en repositorio | ✅ Ejecutado |
| `APP_ENV=production` en Render | ✅ Configurado |
| `APP_SECRET` ≥ 32 chars en Render | ✅ Configurado |
| `DATABASE_URL` usa Internal Database URL | ✅ Configurado |
| `DB_SSLMODE=require` en Render | ✅ Configurado |
| HTTPS activo con certificado válido | ✅ Confirmado en Render |
| Redirect HTTP → HTTPS habilitado | ✅ Confirmado en Render |
| Backups automáticos activos | ✅ Confirmado en Render |
| Plan de DB no es Free Tier | ✅ Confirmado |
| `/healthz` responde `{"status":"ok"}` | ✅ Validado post-deploy |
| `/readyz` responde `{"status":"ok","database":"ok"}` | ✅ Validado post-deploy |
| Logs de deploy sin credenciales en texto claro | ✅ Revisado |
| `ADMIN_EMAIL` y `ADMIN_PASSWORD` eliminados de Render | ✅ Eliminados después del bootstrap |
| `ALLOW_LEGACY_PLAINTEXT_PASSWORDS` = `false` o no definida | ✅ Configurado |

### Clasificación de estado

| Estado | Cuándo aplica |
|---|---|
| 🔴 **No apto para producción** | Falta SMTP rotada, o `APP_ENV` no configurado, o `APP_SECRET` ausente, o `/readyz` falla |
| ⚠️ **Apto con pendientes menores** | Todo lo anterior OK, pero purga del historial pendiente (credencial ya rotada, riesgo mitigado) |
| ✅ **Apto para producción** | Todos los ítems de la tabla anteriores en estado "Ejecutado" o "Confirmado" |

### Estado al momento de este documento

**🔴 No apto para producción**

Pendientes bloqueantes:
1. Credencial SMTP no rotada.
2. Variables de entorno no configuradas en Render.
3. Deploy no ejecutado.
4. `/healthz` y `/readyz` no validados.

---

## Registro de cierre

Completar esta sección al finalizar cada paso:

| Paso | Descripción | Fecha de ejecución | Ejecutado por | Observaciones |
|---|---|---|---|---|
| 0 | Commit de cierre de seguridad | | | |
| 1 | Rotación credencial SMTP | | | |
| 2 | Purga historial de Git | | | |
| 3 | Variables configuradas en Render | | | |
| 4 | Deploy ejecutado | | | |
| 5 | Validaciones post-deploy completadas | | | |
| 6 | Evidencias guardadas | | | |
| — | **Aprobación final** | | | |

**Firma de aprobación técnica:** ___________________________  
**Fecha de aprobación:** ___________________________

---

*Procedimiento operativo de cierre de seguridad — SIEM Suizo Argentina — mayo 2026*
