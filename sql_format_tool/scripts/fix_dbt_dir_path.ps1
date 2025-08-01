$folders = Get-ChildItem -Path (Get-Location) -Directory -Recurse | Where-Object { $_.Name -eq 'dbt' }
if ($folders) {
    $shortest = $folders | Sort-Object { $_.FullName.Length } | Select-Object -First 1

    $configFile = Join-Path -Path (Get-Location) -ChildPath '.\sql_format_tool\resources\.sqlfluff'
    if (Test-Path $configFile) {
        # Get relative path from current directory to dbt folder, using forward slashes
        $dbtFullPath = $shortest.FullName
        $dbtRelativePath = Resolve-Path -Relative -Path $dbtFullPath
        $dbtRelativePath = $dbtRelativePath -replace '\\', '/'
        Write-Output "DBT directory found: $dbtRelativePath"

        # Read all lines into memory
        $content = Get-Content $configFile
        # Only replace the exact line that starts with 'project_dir = ../../'
        $updated = $content | ForEach-Object {
            if ($_ -match '^\s*project_dir\s*=.*$') {
                "project_dir = $dbtRelativePath"
            } else {
                $_
            }
        }
        $updated | Set-Content $configFile
        Write-Output "Replaced path to DBT dir with '$dbtRelativePath/' in $configFile"
    } else {
        Write-Output "Config file not found:"
    }
}