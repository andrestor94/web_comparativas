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
python -m uvicorn web_comparativas.main:app --host 127.0.0.1 --port 8000 --reload
pause
