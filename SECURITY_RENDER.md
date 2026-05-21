# Guía de Seguridad y Operación en Render — SIEM Suizo Argentina

**Clasificación:** Confidencial / Técnico  
**Sistema:** SIEM (web-comparativas-app)  
**Fecha:** 21 de mayo de 2026

---

## Variables de entorno requeridas en producción

Configurar en: **Render Dashboard → Servicio web → Environment**

| Variable | Obligatoria en prod | Formato / Ejemplo | Observación |
|---|---|---|---|
| `APP_ENV` | Sí | `production` | Activa todas las protecciones. Sin esto, la app opera en modo desarrollo. |
| `DATABASE_URL` | Sí | `postgresql://user:pass@host/db` | Provista por Render si la DB está en la misma cuenta. Usar Internal URL. |
| `APP_SECRET` | Sí | Cadena aleatoria ≥ 32 chars | Generar con: `python -c "import secrets; print(secrets.token_hex(32))"` |
| `DB_SSLMODE` | Recomendada | `require` | Default en producción: `require`. No cambiar sin prueba previa. |
| `ADMIN_EMAIL` | Solo bootstrap | `admin@empresa.com` | Solo en el primer arranque. Eliminar después. |
| `ADMIN_PASSWORD` | Solo bootstrap | ≥ 12 caracteres | Solo en el primer arranque. Eliminar después. |
| `ALLOW_LEGACY_PLAINTEXT_PASSWORDS` | No | `false` | Mantener `false`. Solo `true` en migración controlada. |

**Variables de SMTP** (si aplica):
- Configurar `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS` como variables de entorno en Render.
- **NUNCA** almacenarlas en archivos versionados ni en el repositorio.

---

## Alerta: credenciales SMTP en historial de Git

**Estado:** El archivo `web_comparativas/.env` contenía credenciales SMTP reales y estaba versionado en Git.

**Acción tomada:** El archivo fue destrackeado con `git rm --cached web_comparativas/.env`. El archivo local queda intacto pero ya no aparecerá en nuevos commits.

**Pendiente crítico:** Las credenciales SMTP (`SMTP_USER`, `SMTP_PASS`) siguen visibles en el **historial de Git anterior**. Se deben tomar las siguientes acciones:

1. **Rotar de inmediato** la contraseña de `andres.torres@suizoargentina.com` en el panel de correo corporativo (Office 365 / Exchange).
2. **Purgar del historial de Git** usando BFG Repo Cleaner o `git filter-repo` (requiere coordinación del equipo):
   ```bash
   # Ejemplo con BFG (ejecutar en entorno aislado)
   bfg --delete-files .env
   git reflog expire --expire=now --all
   git gc --prune=now --aggressive
   git push --force
   ```
3. **Notificar** a todos los colaboradores del repositorio para que sincronicen después del push forzado.

---

## Configuración de seguridad en Render Dashboard

### HTTPS y Certificados SSL
- [ ] Verificar que el servicio web tiene certificado SSL activo.
- [ ] Habilitar **"Redirect HTTP to HTTPS"** en la configuración del servicio.

### Base de datos PostgreSQL
- [ ] Usar la **Internal Database URL** (host `dpg-xxxxxx-a`): la DB no queda expuesta a internet.
- [ ] Si se usa External URL, confirmar que `DB_SSLMODE=require` y que Render exige SSL en la DB.
- [ ] Confirmar que `DATABASE_URL` en el servicio apunta a la base de producción correcta.

### Backups
- [ ] Confirmar en **Render → PostgreSQL → Backups** que los backups automáticos estén activos.
- [ ] Verificar que el plan de DB **no es Free Tier** (sin backups, se borra a los 90 días).
- [ ] Confirmar retención mínima de 7 días.
- [ ] Hacer un simulacro de restauración en staging para validar funcionalidad.

---

## Cómo ejecutar scripts administrativos de forma segura

### Crear/resetear el administrador
```bash
# Interactivo — pide contraseña de forma segura
python reset_admin.py

# Con variable de entorno (para CI/Render)
ADMIN_EMAIL=admin@suizo.com ADMIN_INITIAL_PASSWORD=MiClaveSegura2026! python reset_admin.py
```

### Seed del administrador
```bash
ADMIN_SEED_EMAIL=admin@suizo.com ADMIN_SEED_PASSWORD=MiClaveSegura2026! python seed_admin.py
```

**Reglas:**
- Contraseñas mínimo 12 caracteres. El script valida y rechaza si no se cumple.
- Nunca ejecutar scripts apuntando a la DB de producción desde una PC local.
- La guardia de seguridad en `models.py` bloquea el arranque si `DATABASE_URL` apunta a Render desde entorno local.

---

## Migración de contraseñas legacy (texto plano)

Si existen usuarios con contraseñas sin hashear en la base de datos:

1. Identificar:
   ```sql
   SELECT id, email FROM users WHERE password_hash NOT LIKE '$%';
   ```
2. Habilitar temporalmente el fallback (solo en staging):
   ```
   ALLOW_LEGACY_PLAINTEXT_PASSWORDS=true
   ```
3. Los usuarios afectados deben cambiar su contraseña al próximo login.
4. Una vez migrados todos, volver a `ALLOW_LEGACY_PLAINTEXT_PASSWORDS=false`.
5. **Nunca** dejar `true` en producción de forma permanente.

---

## Health checks y monitoreo

```bash
# Liveness (la app está viva)
GET /healthz
Respuesta: {"status": "ok"}

# Readiness (valida DB con SELECT 1)
GET /readyz
Respuesta OK:      {"status": "ok", "database": "ok"}
Respuesta error:   {"status": "degraded", "database": "error"}  HTTP 503
```

Render usa `/healthz` como liveness probe. Configurar alertas externas (UptimeRobot, Sentry) sobre `/readyz` para detectar caídas de base de datos.

---

## Pendientes de validación manual en Render

Los siguientes puntos deben verificarse directamente en el panel de Render — no son verificables desde el código:

- [ ] Confirmar `APP_ENV=production` en variables de entorno del servicio.
- [ ] Confirmar `APP_SECRET` definido con valor aleatorio ≥ 32 caracteres.
- [ ] Confirmar que `DATABASE_URL` usa la **Internal Database URL** (host sin `.render.com`).
- [ ] Confirmar `DB_SSLMODE=require` en variables de entorno.
- [ ] Confirmar HTTPS activo con certificado válido.
- [ ] Confirmar **Redirect HTTP → HTTPS** habilitado.
- [ ] Confirmar backups automáticos activos y retención ≥ 7 días.
- [ ] Confirmar que el plan de la DB no es Free Tier.
- [ ] Confirmar que los logs de Render no muestran contraseñas, tokens ni connection strings.
- [ ] Confirmar que `ADMIN_EMAIL` y `ADMIN_PASSWORD` fueron eliminados después del bootstrap inicial.
- [ ] Confirmar que las credenciales SMTP fueron rotadas (ver sección de alerta arriba).
- [ ] Confirmar que el historial de Git fue purgado (`.env` con credenciales SMTP).

---

## Cómo generar secretos seguros

```bash
# APP_SECRET (32 bytes = 64 caracteres hex)
python -c "import secrets; print(secrets.token_hex(32))"

# Contraseña de admin inicial (24 caracteres)
python -c "import secrets, string; print(secrets.token_urlsafe(18))"
```

---

## Rotación de credenciales si hubo exposición

Si algún secreto estuvo expuesto (historial Git público, logs, etc.):

1. Rotar la contraseña SMTP en el panel de Office 365/Exchange.
2. Rotar `APP_SECRET` en Render (esto invalida todas las sesiones activas).
3. Rotar la contraseña de la base de datos en Render → PostgreSQL → Credentials.
4. Verificar que los nuevos valores estén correctamente configurados como variables de entorno en Render.
5. Reiniciar el servicio en Render.
6. Verificar `/readyz` responde `{"status": "ok", "database": "ok"}`.
