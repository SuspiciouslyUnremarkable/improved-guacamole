# Get the repo name from the current directory
$repoName = "sql_format_tool"
$venvPath = Join-Path -Path "." -ChildPath "venv\$repoName"

# Get PYTHON_PATH from the environment variables
$pythonPath = $env:PYTHON_PATH
if (-not $pythonPath) {
    Write-Host "PYTHON_PATH environment variable is not set. Please add it to your .vscode/settings.json file. as:"
    Write-Host '  "PYTHON_PATH": "C:/Path/To/Python/python.exe"'
    exit 1
}


# Create the venv directory if it doesn't exist
if (-not (Test-Path -Path $venvPath)) {
    & $pythonPath -m venv $venvPath
    Write-Host "Virtual environment created at $venvPath"
} else {
    Write-Host "Virtual environment already exists at $venvPath"
}

# Activate the virtual environment
$activateScript = Join-Path -Path $venvPath -ChildPath "Scripts\Activate.ps1"
if (Test-Path -Path $activateScript) {
    . $activateScript
    Write-Host "Virtual environment activated."
} else {
    Write-Host "Activation script not found at $activateScript"
}

# Install requirements
$requirementsFile = "sql_format_tool\scripts\requirements.txt"
if (Test-Path -Path $requirementsFile) {    
    pip install -r $requirementsFile
    Write-Host "Requirements installed from $requirementsFile"
} else {
    Write-Host "Requirements file not found at $requirementsFile"
}
