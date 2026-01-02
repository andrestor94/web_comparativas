@echo off
setlocal enableextensions enabledelayedexpansion
:: ==========================================
:: SCRIPT: subir_a_github_aqui.bat
:: Autor: Andrés Torres (adaptado)
:: Desc: Sube cambios del repo en la carpeta actual (o donde está el .bat)
:: Uso:  doble clic, o: subir_a_github_aqui.bat "tu mensaje de commit"
:: ==========================================

:: 1) Elegir repo: si la carpeta actual tiene .git, usarla; si no, usar la carpeta del .bat
if exist ".git" (
  set "REPO=%CD%"
) else (
  set "REPO=%~dp0"
)

if not exist "%REPO%\.git" (
  echo [ERROR] No se encontro un repo Git en:
  echo   - Carpeta actual: %CD%
  echo   - Carpeta del script: %~dp0
  echo Aborta.
  pause
  exit /b 1
)

pushd "%REPO%" >nul
git status --porcelain >nul 2>&1 || (
  echo [ERROR] Git no esta inicializado en: %REPO%
  popd >nul
  pause
  exit /b 1
)

:: 2) Mensaje de commit (si pasas argumento, lo usa)
set "MSG=%*"
if "%MSG%"=="" set "MSG=Actualizacion rapida - %date% %time:~0,5%"

:: 3) Detectar rama actual (fallback a main)
for /f "delims=" %%b in ('git rev-parse --abbrev-ref HEAD') do set "BRANCH=%%b"
if "%BRANCH%"=="" set "BRANCH=main"

echo ==========================================
echo Repo : %REPO%
echo Rama : %BRANCH%
echo Msg  : %MSG%
echo ==========================================

:: 4) Agregar y commitear solo si hay cambios
git add -A

git diff --cached --quiet
if errorlevel 1 (
  git commit -m "%MSG%"
) else (
  echo [INFO] No hay cambios para commitear.
)

:: 5) Asegurar upstream y sincronizar con rebase
git rev-parse --symbolic-full-name --abbrev-ref --quiet @{u} >nul 2>&1
if errorlevel 1 (
  echo [INFO] No hay upstream configurado. Creando upstream en origin/%BRANCH%...
  git push -u origin "%BRANCH%" || goto :error
) else (
  echo [INFO] Haciendo pull --rebase desde origin/%BRANCH%...
  git pull --rebase origin "%BRANCH%" || goto :error
  echo [INFO] Pushing a origin/%BRANCH%...
  git push origin "%BRANCH%" || goto :error
)

echo.
echo ==========================================
echo  OK: Cambios subidos a origin/%BRANCH%.
echo  Esto dispara el deploy en Render si tu servicio esta vinculado.
echo ==========================================
popd >nul
exit /b 0

:error
echo.
echo ==========================================
echo  ERROR sincronizando con origin/%BRANCH%.
echo  Revisar credenciales, permisos o conflictos de merge.
echo ==========================================
popd >nul
exit /b 1
