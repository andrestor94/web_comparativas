@echo off
:: ==========================================
:: SCRIPT: subir_a_github_rapido.bat
:: Autor: Andrés Torres
:: Descripción: Subida automática de cambios a GitHub
:: ==========================================

:: Ruta del proyecto
cd "C:\Users\ANDRES.TORRES\OneDrive - Suizo Argentina S.A\web_comparativas"

:: Obtener fecha y hora actuales
for /f "tokens=1-4 delims=/ " %%a in ('date /t') do (
    set fecha=%%a-%%b-%%c
)
for /f "tokens=1-2 delims=: " %%a in ('time /t') do (
    set hora=%%a-%%b
)

echo ==========================================
echo  Subiendo cambios al repositorio GitHub...
echo ==========================================
echo.

:: Agregar todos los cambios
git add .

:: Crear commit con fecha y hora
git commit -m "Actualización automática – %fecha% %hora%"

:: Subir los cambios
git push origin main

echo.
echo ✅ Cambios subidos correctamente al repositorio.
echo 📅 Fecha: %fecha% ⏰ Hora: %hora%
echo ==========================================
echo Presiona cualquier tecla para cerrar...
pause >nul
