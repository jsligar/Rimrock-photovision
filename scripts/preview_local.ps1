Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot

$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
  throw "Missing .venv Python at $venvPython. Create it first with: python -m venv .venv"
}

$runtime = Join-Path $repoRoot ".runtime"
$nas = Join-Path $runtime "nas"
$originals = Join-Path $runtime "originals"
$organized = Join-Path $runtime "organized"
$crops = Join-Path $runtime "crops"
New-Item -ItemType Directory -Force $runtime, $nas, $originals, $organized, $crops | Out-Null

$env:LOCAL_BASE = $runtime
$env:NAS_SOURCE_DIR = $nas
$env:API_PORT = "8420"

Write-Host "Rimrock local preview starting..."
Write-Host "LOCAL_BASE=$($env:LOCAL_BASE)"
Write-Host "NAS_SOURCE_DIR=$($env:NAS_SOURCE_DIR)"
Write-Host "URL: http://127.0.0.1:8420"
Write-Host "Press Ctrl+C to stop."
Write-Host ""

& $venvPython -m api.main
