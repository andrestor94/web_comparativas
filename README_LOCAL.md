# README LOCAL — SIEM Web Comparativas

## Levantar el servidor local

1. Abrir la carpeta del proyecto:
   ```
   C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\
   ```
2. Doble clic en `start_web_comparativas.bat`
3. Esperar hasta ver en la terminal:
   ```
   Uvicorn running on http://127.0.0.1:8000
   ```
4. Abrir el navegador en: **http://127.0.0.1:8000**

---

## Entrar a la app

| Usuario | Password | Rol |
|---------|----------|-----|
| admin@suizo.com | admin123 | admin |
| admin@local | (desconocida — resetear si es necesario) | admin |
| andrestor94@hotmail.com | (desconocida — resetear si es necesario) | auditor |

Para resetear una password, usar el panel:
```
http://127.0.0.1:8000/sic/users
```

---

## Verificar Git

```bat
cd C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok
git status
git remote -v
git log --oneline -5
```

El remote correcto es:
```
origin  https://github.com/andrestor94/web_comparativas.git
```

---

## Subir cambios a GitHub (y disparar deploy en Render)

```bat
subir_a_github_aqui.bat "mensaje del commit"
```

O simplemente doble clic en `subir_a_github_aqui.bat` para usar mensaje automático.

Después de un push exitoso, Render redespliega automáticamente (demora ~2-3 min).

---

## Qué NO tocar

| Archivo/Carpeta | Por qué |
|-----------------|---------|
| `web_comparativas\app.db` | Base de datos local activa (843 MB) |
| `web_comparativas\.env` | Variables de entorno locales (no va a GitHub) |
| `render.yaml` | Configuración del deploy en Render |
| `requirements.txt` | Dependencias del proyecto |
| `venv_webcomparativas\` | Entorno virtual Python (no va a GitHub) |
| `_backup_revision_siem_*\` | Backups históricos — no modificar |

---

## Qué hacer si vuelve a aparecer WinError 10013

**Causa:** el puerto 8000 está ocupado por un proceso anterior de Python/uvicorn.

**Solución rápida:**

1. Abrir PowerShell y ejecutar:
   ```powershell
   Get-NetTCPConnection -LocalPort 8000 | Select-Object OwningProcess
   ```
2. Anotar el PID que aparece.
3. Ejecutar:
   ```powershell
   Stop-Process -Id <PID> -Force
   ```
4. Volver a abrir `start_web_comparativas.bat`.

**Solución alternativa:** cerrar todas las ventanas de terminal y reintentar.

El `start_web_comparativas.bat` ya está configurado con `--host 127.0.0.1 --port 8000` para evitar problemas de permisos de firewall.

---

## Qué hacer antes de pedirle cambios a una IA

1. Confirmar que estás trabajando en la carpeta correcta:
   ```
   C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\
   ```
2. Ejecutar `git status` — los cambios deben aparecer en esa carpeta.
3. Verificar que el servidor local corre desde esa misma carpeta.
4. Si hay dudas, decirle a la IA: _"La carpeta activa es web_comparativas_v2- ok"_.

---

## Estructura del proyecto

```
web_comparativas_v2- ok\
├── web_comparativas\          ← código fuente del backend
│   ├── main.py                ← punto de entrada de la app
│   ├── models.py              ← modelos de base de datos
│   ├── app.db                 ← base de datos local (~843 MB)
│   ├── routers\               ← endpoints por módulo
│   ├── templates\             ← HTML de la interfaz
│   └── static\                ← CSS, JS, imágenes
├── venv_webcomparativas\      ← entorno virtual Python (no en git)
├── render.yaml                ← configuración de deploy
├── requirements.txt           ← dependencias Python
├── start_web_comparativas.bat ← arrancar local
├── subir_a_github_aqui.bat    ← subir a GitHub
├── PROJECT_ACTIVE.md          ← referencia rápida del proyecto
└── README_LOCAL.md            ← este archivo
```

---

_Última actualización: 2026-05-21_
