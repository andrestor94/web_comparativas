# PROYECTO ACTIVO — SIEM Web Comparativas

## Carpeta activa oficial

```
C:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\
```

Esta es la **única** carpeta válida del proyecto. No trabajar en ninguna otra.

---

## Arranque local

Doble clic en:
```
start_web_comparativas.bat
```

URL de acceso:
```
http://127.0.0.1:8000
```

Credencial admin local:
```
admin@suizo.com  /  admin123
```

---

## Repositorio GitHub

```
https://github.com/andrestor94/web_comparativas.git
```

Rama principal: `main`

---

## Deploy en producción

Plataforma: **Render**
Configuración: `render.yaml` en la raíz del proyecto
Trigger: push a `main` en GitHub → Render redespliega automáticamente

Script de subida:
```
subir_a_github_aqui.bat   ← subida con commit manual
subir_a_github_rapido.bat ← subida rápida
```

---

## Base de datos local activa

```
web_comparativas\app.db   (~843 MB)
```

Esta base contiene todos los datos locales reales.
No renombrar, mover ni reemplazar sin hacer backup previo.

---

## Módulos principales

| Módulo | Ruta local |
|--------|-----------|
| Forecast | http://127.0.0.1:8000/forecast |
| Comparativa / Uploads | http://127.0.0.1:8000/mercado-publico/web-comparativas |
| Lectura de Pliegos | http://127.0.0.1:8000/mercado-publico/lectura-pliegos |
| Dimensionamiento | http://127.0.0.1:8000/api/mercado-privado/dimensiones/bootstrap |
| SIC / Dashboard | http://127.0.0.1:8000/sic/ |
| Panel usuarios | http://127.0.0.1:8000/sic/users |

---

## Entorno virtual

```
venv_webcomparativas\   (Python 3.10.11, en .gitignore)
```

Si el venv se pierde, recrear con:
```bat
py -3.10 -m venv venv_webcomparativas
venv_webcomparativas\Scripts\pip install -r requirements.txt
```

---

## Reglas obligatorias

1. **Todo cambio de código** se hace únicamente dentro de esta carpeta activa.
2. **Nunca duplicar** la carpeta del proyecto para "hacer una versión nueva". Usar ramas Git.
3. **No trabajar** en carpetas dentro de `_backup_revision_siem_*` ni en la cuarentena.
4. **Antes de pedir cambios a una IA**, verificar que el terminal/editor esté apuntando a esta carpeta.
5. **Antes de hacer `git push`**, ejecutar `git status` y confirmar que los cambios son los esperados.
6. **No subir** `.env`, `app.db`, ni `venv_webcomparativas/` a GitHub (ya están en `.gitignore`).

---

## Backups disponibles

Ubicación:
```
C:\Users\ANDRES.TORRES\Desktop\_backup_revision_siem_20260521\
```

Contiene copias antiguas del proyecto y backups de bases de datos.
No eliminar sin revisión manual.

---

_Última actualización: 2026-05-21_
