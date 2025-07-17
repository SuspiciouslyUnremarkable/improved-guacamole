# Activate the Python virtual environment and run format_sql.py

$venvPath = "venv\improved-guacamole\Scripts\Activate.ps1"
$scriptPath = ".\format_sql.py"

if (Test-Path $venvPath) {
    & $venvPath
    python $scriptPath
} else {
    Write-Error "Virtual environment not found at $venvPath"
}