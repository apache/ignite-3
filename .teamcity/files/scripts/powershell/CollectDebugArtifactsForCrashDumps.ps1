$dumpsDir = "%PATH__CRASH_DUMPS%"
$binDir = "%PATH__CMAKE_BUILD_DIRECTORY%\Debug\bin"

if (-not (Test-Path $dumpsDir)) {
    Write-Host "Dumps directory '$dumpsDir' does not exist, skipping."
    exit 0
}

$dumps = Get-ChildItem -Path $dumpsDir -File -Filter "*.dmp"
if ($dumps.Count -eq 0) {
    Write-Host "Dumps directory '$dumpsDir' is empty, skipping."
    exit 0
}

Write-Host "Found $($dumps.Count) dump file(s), collecting binaries from CMake build directory."

if (-not (Test-Path $binDir)) {
    Write-Error "Bin directory '$binDir' does not exist."
    exit 1
}

$dumpNames = $dumps | ForEach-Object { ($_.BaseName -split "\.exe")[0] }

$exes = @(Get-ChildItem -Path "$binDir\*" -File -Include "*.exe" |
    Where-Object { $dumpNames -contains $_.BaseName })

$dlls = @(Get-ChildItem -Path "$binDir\*" -File -Include "*.dll")

$binaryNames = @(($exes + $dlls) | ForEach-Object { $_.BaseName })

$pdbs = @(Get-ChildItem -Path "$binDir\*" -File -Include "*.pdb" |
    Where-Object { $binaryNames -contains $_.BaseName })

$filesToCopy = @($exes + $dlls + $pdbs)
if ($filesToCopy.Count -eq 0) {
    Write-Host "Warning: no matching binaries found in '$binDir'."
    exit 0
}

$filesToCopy | ForEach-Object {
    Copy-Item -Path $_.FullName -Destination $dumpsDir -Force
    Write-Host "Copied: $($_.Name)"
}
