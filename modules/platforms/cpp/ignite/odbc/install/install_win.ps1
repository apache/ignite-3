param (
    [Parameter(Mandatory = $true, Position = 0, HelpMessage = "Mode of operation: 'install' or 'remove'.")]
    [ValidateSet("install", "remove")]
    [string]$Mode,

    [Parameter(Position = 1, HelpMessage = "Absolute path to the ODBC driver file. Required for 'install' mode.")]
    [string]$DriverPath
)

# Treat all errors as terminating so unhandled failures always produce a non-zero exit code.
$ErrorActionPreference = 'Stop'

# Check for administrator privileges explicitly so the error is clear in the build log.
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)
if (-not $isAdmin) {
    Write-Error "This script must be run as Administrator. Ensure the TeamCity build agent service account has the required privileges."
    exit 1
}

$DriverName  = "Apache Ignite 3"
$OdbcInstKey = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\$DriverName"
$OdbcDrvKey  = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"

# ============================================================
# INSTALL
# ============================================================
if ($Mode -eq "install") {

    # --- Validate DriverPath was supplied ---
    if (-not $DriverPath) {
        Write-Error "DriverPath is required in 'install' mode. Call format: .\install_win.ps1 install <abs_path_to_driver>"
        exit 1
    }

    # --- Validate the path points to an existing file (not a directory) ---
    if (-not (Test-Path -LiteralPath $DriverPath)) {
        Write-Error "Driver cannot be found: $DriverPath"
        exit 1
    }

    if (Test-Path -LiteralPath $DriverPath -PathType Container) {
        Write-Error "The path '$DriverPath' is a directory. You must specify the path to the driver file itself, not its folder."
        exit 1
    }

    # --- Install the driver ---
    Write-Host "Installing ODBC driver: $DriverPath"

    # Create the driver sub-key if it doesn't exist
    if (-not (Test-Path -LiteralPath $OdbcInstKey)) {
        New-Item -Path $OdbcInstKey -Force | Out-Null
    }

    # Write driver properties
    New-ItemProperty -LiteralPath $OdbcInstKey -Name "DriverODBCVer" -Value "03.80"     -PropertyType String -Force | Out-Null
    New-ItemProperty -LiteralPath $OdbcInstKey -Name "UsageCount"    -Value 1           -PropertyType DWord  -Force | Out-Null
    New-ItemProperty -LiteralPath $OdbcInstKey -Name "Driver"        -Value $DriverPath -PropertyType String -Force | Out-Null
    New-ItemProperty -LiteralPath $OdbcInstKey -Name "Setup"         -Value $DriverPath -PropertyType String -Force | Out-Null

    # Register the driver name in the ODBC Drivers list
    if (-not (Test-Path -LiteralPath $OdbcDrvKey)) {
        New-Item -Path $OdbcDrvKey -Force | Out-Null
    }
    New-ItemProperty -LiteralPath $OdbcDrvKey -Name $DriverName -Value "Installed" -PropertyType String -Force | Out-Null

    Write-Host "Driver '$DriverName' installed successfully."
}

# ============================================================
# REMOVE
# ============================================================
elseif ($Mode -eq "remove") {

    $removedSomething = $false

    # Remove the driver properties key
    if (Test-Path -LiteralPath $OdbcInstKey) {
        Remove-Item -LiteralPath $OdbcInstKey -Recurse -Force
        Write-Host "Removed registry key: $OdbcInstKey"
        $removedSomething = $true
    } else {
        Write-Warning "Driver key not found (already removed?): $OdbcInstKey"
    }

    # Remove the entry from the ODBC Drivers list
    $drvProperty = Get-ItemProperty -LiteralPath $OdbcDrvKey -Name $DriverName -ErrorAction SilentlyContinue
    if ($null -ne $drvProperty) {
        Remove-ItemProperty -LiteralPath $OdbcDrvKey -Name $DriverName -Force
        Write-Host "Removed '$DriverName' from ODBC Drivers list."
        $removedSomething = $true
    } else {
        Write-Warning "Driver entry not found in ODBC Drivers list (already removed?): $DriverName"
    }

    if ($removedSomething) {
        Write-Host "Driver '$DriverName' removed successfully."
    } else {
        Write-Host "Nothing to remove - driver '$DriverName' was not registered."
    }
}
