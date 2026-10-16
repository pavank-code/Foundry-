$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $here

if (-not $env:SMART_CITY_DATA_DIR) {
    if (Test-Path "C:\Users\haard\Downloads\03_smart_city_where_fix_first") {
        $env:SMART_CITY_DATA_DIR = "C:\Users\haard\Downloads\03_smart_city_where_fix_first"
    } else {
        $env:SMART_CITY_DATA_DIR = Join-Path $here "data"
    }
}

# Find valid python executable
$venvPython = Join-Path $here "..\work\venv\Scripts\python.exe"
$pythonExe = $null

if (Test-Path $venvPython) {
    $pythonExe = (Resolve-Path $venvPython).Path
}

if (-not $pythonExe) {
    $pyCmd = Get-Command py -ErrorAction SilentlyContinue
    if ($pyCmd -and $pyCmd.Source -notlike "*WindowsApps*") {
        $pythonExe = $pyCmd.Source
    }
}

if (-not $pythonExe) {
    $pyCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pyCmd -and $pyCmd.Source -notlike "*WindowsApps*") {
        $pythonExe = $pyCmd.Source
    }
}

if (-not $pythonExe) {
    $uvPy = "C:\Users\haard\AppData\Roaming\uv\python\cpython-3.11.16-windows-x86_64-none\python.exe"
    if (Test-Path $uvPy) {
        $pythonExe = $uvPy
    }
}

if (-not $pythonExe) {
    throw "Python executable could not be found. Please ensure Python is installed."
}

Write-Host "Using Python: $pythonExe"
Write-Host "Data directory: $env:SMART_CITY_DATA_DIR"
Write-Host ""
Write-Host "=========================================================="
Write-Host " Starting Smart City Apache Spark & NVIDIA NIM Service"
Write-Host " URL: http://127.0.0.1:8765/"
Write-Host "=========================================================="
Write-Host ""

& $pythonExe -m uvicorn smart_city_app:app --host 127.0.0.1 --port 8765
