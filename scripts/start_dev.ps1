$ErrorActionPreference = "Stop"

if (-not (Test-Path ".venv")) {
    Write-Host "Virtualenv missing. Run scripts/setup_dev.ps1 first." -ForegroundColor Yellow
    exit 1
}

$venvActivate = Join-Path $PSScriptRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
    Write-Host "Activating Windows virtualenv..." -ForegroundColor Cyan
    & $venvActivate
} else {
    Write-Host "Activate virtualenv manually if needed (POSIX layout detected)." -ForegroundColor Yellow
}

if (-not (Get-Command uvicorn -ErrorAction SilentlyContinue)) {
    Write-Host "uvicorn not found. Installing requirements..." -ForegroundColor Yellow
    pip install -r requirements.txt
}

Write-Host "Launching FastAPI (mock) on port 8001" -ForegroundColor Green
$backendCommand = "& { Set-Location '$PWD'; `$env:APP_MODE='MOCK'; uvicorn modern_backend.main:app --reload --port 8001 }"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCommand

Write-Host "Launching frontend Vite (port 5173)" -ForegroundColor Green
if (-not (Test-Path "modern_frontend\node_modules")) {
    Push-Location modern_frontend
    npm install
    Pop-Location
}
$frontendCommand = "& { Set-Location '$PWD\modern_frontend'; npm run dev }"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $frontendCommand

Write-Host 'All processes started. Backend: http://localhost:8001 , Frontend: http://localhost:5173' -ForegroundColor Cyan
