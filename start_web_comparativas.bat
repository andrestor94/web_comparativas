@echo off
title Web Comparativas - MODO RAPIDO (summary ON)
echo ========================================
echo Iniciando Web Comparativas - MODO RAPIDO (summary ON)
echo Inflacion + Laboratorio leen tablas precalculadas (sin VPN)
echo ========================================
cd /d "%~dp0"

:: === FLAG global del modulo Indicadores Comerciales: enciende la lectura summary ===
:: Hoy cubre las pestanias Inflacion e Informes de Laboratorio. Rentabilidad Negativa
:: sigue leyendo en vivo (todavia no esta cableada al summary).
set "INDICADORES_USE_SUMMARY=1"

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
echo  (Inflacion y Laboratorio leen de tablas precalculadas: INDICADORES_USE_SUMMARY=1)
echo.
:: NOTA: SIN --reload. El watcher de --reload vigila TODA la raiz del proyecto de
:: forma recursiva; con _backups_db_local (~4.8 GB), app.db (~1.1 GB), data (~418 MB)
:: y el venv, el escaneo/vigilancia satura CPU/RAM y TILDA la PC (no muestra la UI).
:: Para usar la app NO hace falta autorecarga. Si necesitas hot-reload para desarrollo,
:: usa la linea comentada de abajo, que acota el watcher SOLO al codigo fuente.
python -m uvicorn web_comparativas.main:app --host 127.0.0.1 --port 8000
:: DEV (hot-reload acotado, opcional): vigila solo el paquete y excluye datos/DB pesados
:: python -m uvicorn web_comparativas.main:app --host 127.0.0.1 --port 8000 --reload --reload-dir web_comparativas --reload-exclude "*.db" --reload-exclude "*.db-journal" --reload-exclude "data/*"
pause
