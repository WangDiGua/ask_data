param(
    [string]$ListenHost = "127.0.0.1",
    [int]$Port = 8001,
    [switch]$Reload
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$venvFastMcp = Join-Path $repoRoot ".venv\Scripts\fastmcp.exe"
$srcPath = Join-Path $repoRoot "src"

if (Test-Path $venvPython) {
    $pythonExe = $venvPython
} else {
    $pythonCommand = Get-Command python -ErrorAction Stop
    $pythonExe = $pythonCommand.Source
}

if (Test-Path $venvFastMcp) {
    $fastMcpExe = $venvFastMcp
} else {
    $fastMcpExe = $null
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

try {
    $runtimeMessage = & $pythonExe -c "from ndea.runtime import runtime_support_message; msg = runtime_support_message(); print(msg or '')"
    if ($LASTEXITCODE -ne 0) {
        throw "failed to inspect runtime"
    }
    $runtimeMessage = ($runtimeMessage | Out-String).Trim()
    if (-not [string]::IsNullOrWhiteSpace($runtimeMessage)) {
        Write-Warning $runtimeMessage
    }
} catch {
    Write-Warning "Unable to inspect Python runtime compatibility: $($_.Exception.Message)"
}

& $pythonExe -c "import fastmcp, ndea" *> $null
if ($LASTEXITCODE -ne 0) {
    throw ".venv 中缺少运行依赖。请执行：.\.venv\Scripts\python.exe -m pip install -e .[dev]"
}

$fastMcpArgs = @(
    "run",
    "src\ndea\main.py:app",
    "--transport", "http",
    "--host", $ListenHost,
    "--port", $Port.ToString(),
    "--no-banner"
)

if ($Reload) {
    $fastMcpArgs += "--reload"
}

Write-Host "[NDEA] 项目目录 : $repoRoot"
Write-Host "[NDEA] Python   : $pythonExe"
Write-Host "[NDEA] 传输方式 : MCP over HTTP"
Write-Host "[NDEA] 服务地址 : http://$ListenHost`:$Port/mcp/"
Write-Host "[NDEA] 日志会持续输出在当前终端，按 Ctrl+C 停止。"

if ($fastMcpExe) {
    & $fastMcpExe @fastMcpArgs
} else {
    & $pythonExe -m fastmcp @fastMcpArgs
}
exit $LASTEXITCODE
