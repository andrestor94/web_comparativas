@echo off
title Iniciando Web Comparativas...
echo ========================================
echo Iniciando Web Comparativas...
echo ========================================
cd /d "%~dp0"
:: === variables para mail y URLs ===
set "APP_PUBLIC_URL=http://127.0.0.1:8000"
set "APP_LOGO_URL=http://127.0.0.1:8000/static/img/logo-suizo.png"

set "MAIL_FROM_NAME=Web Comparativas – Suizo Argentina"
set "MAIL_FROM_EMAIL=no-reply@suizo.com.ar"

set "SMTP_HOST=smtp.tu_proveedor.com"
set "SMTP_PORT=587"
set "SMTP_USER=tu_usuario_smtp"
set "SMTP_PASS=tu_password_smtp"
set "SMTP_TLS=1"
call "venv_webcomparativas\Scripts\activate.bat"
echo.
echo  Abriendo en: http://127.0.0.1:8000
echo.
:: ============================================================================
:: IMPORTANTE — NO AGREGAR --reload  (lee esto antes de "mejorar" el arranque)
:: ----------------------------------------------------------------------------
:: --reload activa el file-watcher de Uvicorn, que vigila TODO el arbol del
:: proyecto (el cwd). Dentro vive web_comparativas\app.db (~1,2 GB) y su journal,
:: que SQLite reescribe constantemente mientras la app corre. El watcher detecta
:: esos cambios -> tormenta de reload + RAM disparada -> la PC se tilda y obliga a
:: apagar en caliente (riesgo de corromper la SQLite).
:: Ya paso antes. Si necesitas autorecarga en DESARROLLO, NO uses --reload pelado:
:: acota el watcher SOLO al codigo y excluye datos pesados, p.ej:
::   --reload --reload-dir web_comparativas --reload-exclude "*.db*" --reload-exclude "data/*"
:: ============================================================================
python -m uvicorn web_comparativas.main:app --host 127.0.0.1 --port 8000
pause
