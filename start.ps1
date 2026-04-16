param(
    [string]$ListenHost = "0.0.0.0",
    [int]$Port = 8001,
    [switch]$Reload
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$srcPath = Join-Path $repoRoot "src"

if (Test-Path $venvPython) {
    $pythonExe = $venvPython
} else {
    $pythonCommand = Get-Command python -ErrorAction Stop
    $pythonExe = $pythonCommand.Source
}

if (-not (Test-Path $srcPath)) {
    throw "Missing source directory: $srcPath"
}

$pythonPathEntries = [System.Collections.Generic.List[string]]::new()
$pythonPathEntries.Add($srcPath)

if ($env:PYTHONPATH) {
    foreach ($entry in $env:PYTHONPATH.Split(';')) {
        if (-not [string]::IsNullOrWhiteSpace($entry)) {
            $pythonPathEntries.Add($entry)
        }
    }
}

$env:PYTHONPATH = ($pythonPathEntries | Select-Object -Unique) -join ';'

& $pythonExe -c "import uvicorn, fastapi, ndea" *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Runtime dependencies are missing in .venv. Run: .\.venv\Scripts\python.exe -m pip install -e .[dev]"
}

$uvicornArgs = @(
    "-m", "uvicorn",
    "ndea.main:http_app",
    "--host", $ListenHost,
    "--port", $Port.ToString()
)

if ($Reload) {
    $uvicornArgs += "--reload"
}

Write-Host "[NDEA] Repo   : $repoRoot"
Write-Host "[NDEA] Python : $pythonExe"
Write-Host "[NDEA] URL    : http://127.0.0.1:$Port"
Write-Host "[NDEA] Logs stream in this terminal. Press Ctrl+C to stop."

& $pythonExe @uvicornArgs
exit $LASTEXITCODE
