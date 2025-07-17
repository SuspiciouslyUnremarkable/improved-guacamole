# Get the repo name from the current directory
$repoName = Split-Path -Leaf (Get-Location)
$venvPath = Join-Path -Path "." -ChildPath "venv\$repoName"

# Create the venv directory if it doesn't exist
if (-not (Test-Path -Path $venvPath)) {
    python -m venv $venvPath
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
$requirementsFile = Join-Path -Path (Get-Location) -ChildPath "requirements.txt"
if (Test-Path -Path $requirementsFile) {    
    pip install -r $requirementsFile
    Write-Host "Requirements installed from $requirementsFile"
} else {
    Write-Host "Requirements file not found at $requirementsFile"
}
