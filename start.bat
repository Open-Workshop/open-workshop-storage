@echo off
setlocal

set SERVICE=%~1
if "%SERVICE%"=="" set SERVICE=distributor

if /I "%SERVICE%"=="distributor" (
    cargo run --bin distributor
    exit /b %errorlevel%
)

if /I "%SERVICE%"=="loader" (
    cargo run --bin loader
    exit /b %errorlevel%
)

echo Usage: %~nx0 [distributor^|loader]
exit /b 1
