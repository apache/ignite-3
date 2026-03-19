$dumpsDir = "%PATH__CRASH_DUMPS%"
$binDir = "%PATH__CMAKE_BUILD_DIRECTORY%\Debug\bin"
$cdb = "C:\Program Files (x86)\Windows Kits\10\Debuggers\x64\cdb.exe"
$symPath = "srv*%PATH__DEBUG_SYMBOLS_DIR%*https://msdl.microsoft.com/download/symbols;$binDir"
$srcPath = "%PATH__WORKING_DIR%"

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

    Write-Host "##teamcity[blockOpened name='Crash analysis: $($dump.Name)']"
    & $cdb -z $dump.FullName -y $symPath -srcpath $srcPath -c ".symopt+ 0x80000; .reload /f; .lines; .ecxr; kL; q"
    Write-Host "##teamcity[blockClosed name='Crash analysis: $($dump.Name)']"
}
