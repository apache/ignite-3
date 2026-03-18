$dumpsDir = "%PATH__CRASH_DUMPS%"
$binDir = "%PATH__CMAKE_BUILD_DIRECTORY%\Debug\bin"
$cdb = "C:\Program Files (x86)\Windows Kits\10\Debuggers\x64\cdb.exe"
$pdbcopy = "C:\Program Files (x86)\Windows Kits\10\Debuggers\x64\pdbcopy.exe"
$symPath = "srv*%PATH__DEBUG_SYMBOLS_DIR%*https://msdl.microsoft.com/download/symbols;$binDir"

if (-not (Test-Path $dumpsDir)) {
    Write-Host "Dumps directory '$dumpsDir' does not exist, skipping."
    exit 0
}

$dumps = @(Get-ChildItem -Path $dumpsDir -File -Filter "*.dmp")
if ($dumps.Count -eq 0) {
    Write-Host "No dump files found in '$dumpsDir', skipping."
    exit 0
}

foreach ($dump in $dumps) {
    Write-Host "##teamcity[buildProblem description='Crash dump detected: $($dump.Name)']"

    Write-Host "##teamcity[blockOpened name='PDB Debug 1: $($dump.Name)']"
    & $pdbcopy $dump.FullName -p
    Write-Host "##teamcity[blockClosed name='PDB Debug 1: $($dump.Name)']"

    Write-Host "##teamcity[blockOpened name='PDB Debug 2: $($dump.Name)']"
    & $cdb -z $dump.FullName -y $symPath -c ".reload /f; lm v m ignite_client_test; q"
    Write-Host "##teamcity[blockClosed name='PDB Debug 2: $($dump.Name)']"

    Write-Host "##teamcity[blockOpened name='Crash analysis: $($dump.Name)']"
    & $cdb -z $dump.FullName -y $symPath -c ".symopt+ 0x80000; .lines; !analyze -v; .ecxr; kn /f; q"
    Write-Host "##teamcity[blockClosed name='Crash analysis: $($dump.Name)']"
}

$pdbs = @(Get-ChildItem -Path $dumpsDir -File -Filter "*.pdb")
if ($pdbs.Count -eq 0) {
    Write-Host "No PDB files found in '$dumpsDir', skipping."
    exit 0
}

foreach ($pdb in $pdbs) {
    Write-Host "##teamcity[blockOpened name='PDB Debug 1: $($pdb.Name)']"
    & $pdbcopy $pdb.FullName -p
    Write-Host "##teamcity[blockClosed name='PDB Debug 1: $($pdb.Name)']"
}