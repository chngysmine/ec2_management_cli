Param()

Write-Host "=== Modern EC2 Manager - Windows Dev Setup ===" -ForegroundColor Cyan

try {
    $pythonVersion = python -c "import sys; print('.'.join(map(str, sys.version_info[:3])))"
    Write-Host ("Python version: {0}" -f $pythonVersion)
}
catch {
    Write-Host "Python not found in PATH. Install Python 3.11+ and retry." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path ".venv")) {
    Write-Host "Creating virtualenv .venv..." -ForegroundColor Yellow
    python -m venv .venv
}

$venvDir = Join-Path $PSScriptRoot ".venv"
$winActivate = Join-Path $venvDir "Scripts\Activate.ps1"
$posixPython = Join-Path $venvDir "bin\python"
$winPython = Join-Path $venvDir "Scripts\python.exe"

if (Test-Path $winActivate) {
    Write-Host "Activating virtualenv (Windows layout)..."
    & $winActivate
}
elseif (Test-Path $posixPython) {
    Write-Host "Detected POSIX-style virtualenv (.venv/bin). Skipping PowerShell activation." -ForegroundColor Yellow
}
else {
    Write-Host "Warning: unable to locate activation script. Continuing..." -ForegroundColor Yellow
}

$venvPython = if (Test-Path $winPython) { $winPython } elseif (Test-Path $posixPython) { $posixPython } else { "python" }

Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install -r requirements.txt

Write-Host ""
Write-Host "Done. To run the FastAPI backend (mock mode):" -ForegroundColor Green
Write-Host "  `$env:APP_MODE = MOCK"
Write-Host "  uvicorn modern_backend.main:app --reload --port 8001"

