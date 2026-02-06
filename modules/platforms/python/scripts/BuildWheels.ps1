# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

<#
.SYNOPSIS
    Builds wheels of the package.
.DESCRIPTION
    The script builds wheels of the package. Should be called from the root directory of the package. The resulting
    wheels are placed to the "distr" directory. In the process, script creates "wheels_build" directory, which can
    be safely removed once script execution is finished.
    For the script to work you should have pyenv-win installed on your machine
    (https://pypi.org/project/pyenv-win/).
    For every of the specified versions, script tries to use the latest local version, and installs the latest if
    there are no local pythons matching the specified version.
.PARAMETER PyVers
    Python versions to use, through coma.
.EXAMPLE
    PS> .\scripts\BuildWheels.ps1 -PyVers "3.10,3.11,3.12,3.13,3.14"
    Build wheels for Python versions 3.10-3.14
.EXAMPLE
    PS> .\scripts\BuildWheels.ps1 -PyVers 3.11
    Build wheels for Python versions 3.11 only
.EXAMPLE
    PS> .\scripts\BuildWheels.ps1 -PyVers 3.11.7
    Build wheels for Python, using version 3.11.7 exactly
#>

param ([Parameter(Mandatory)][string]$PyVers)

$ErrorActionPreference = "Stop"

$WheelsBuildDir="wheels_build"
$WheelsTargetDir="distr"
$PyEvnUpdated=$false

function InfoMessage {
    param ($text)
    Write-Host $text -ForegroundColor DarkGreen
}

function ThrowOnNonZeroExitCode {
    if ($LastExitCode) {
        throw "Error while executing subcommand."
    }
}

function ExecCheckReturnCode {
    param ([string]$command)
    InfoMessage "> $command"
    Invoke-Expression $command
    ThrowOnNonZeroExitCode
}

function TryUpdatePyenv {
    if ($PyEvnUpdated) {
        return $false
    }
    InfoMessage "Trying to update available versions (this can take some time)..."
    ExecCheckReturnCode "pyenv update"
    $PyEvnUpdated=$true
    return $true
}

function FindExactVersion {
    param ([string]$Ver)
    $Result="" | Select-Object -Property Version,Local
    $Result.Local=$false

    if ($Ver -match "^3\.[0-9]+\.[0-9]+$") {
        InfoMessage "The version contains the patch part. Trying to use exactly it."
        $VerPattern=$Ver.Replace(".","\.") + "$"
    } else {
        # Pattern to use for finding the latest patch for the provided version.
        $VerPattern = $Ver.Replace(".", "\.") + "\.[0-9]+$"
    }

    InfoMessage "Trying to find appropriate version among locally installed."
    $PyVersAvailable=ExecCheckReturnCode "pyenv versions" | Select-String -Pattern "$VerPattern"

    if (!$PyVersAvailable) {
        InfoMessage "No locally installed versions for $Ver."
        InfoMessage "Trying to find appropriate version among remotely available."
        $PyVersAvailable=ExecCheckReturnCode "pyenv install -l" | Select-String -Pattern "$VerPattern"
    } else {
        $Result.Local=$true
    }

    if (!$PyVersAvailable) {
        $Updated=TryUpdatePyenv
        if (!$Updated) {
            throw "No python available for $Ver. Are you sure this is a proper version?"
        }

        InfoMessage "Trying to find appropriate version among remotely available again."
        $PyVersAvailable=ExecCheckReturnCode "pyenv install -l" | Select-String -Pattern "$VerPattern"
    }

    if (!$PyVersAvailable) {
        throw "No python available for $Ver. Are you sure this is a proper version?"
    }

    InfoMessage "Choosing among these versions:"
    InfoMessage (($PyVersAvailable.Matches | ForEach-Object{$_.Value}) -join "`n")

    $Result.Version=$PyVersAvailable.Matches[-1].Value.Replace(" ", "")

    return $Result
}

$PyVersList=$PyVers.Replace(" ","").Split(",")

InfoMessage "Checking pyenv version."
try {
    pyenv --version
} catch {
    throw "pyenv is required for the script, but is not found. Please, make sure pyenv-win
     (https://pypi.org/project/pyenv-win) is properly installed and try again"
}

foreach ($Ver in $PyVersList) {
    InfoMessage "Making a wheel for Python $Ver."

    $Resolved = FindExactVersion $Ver
    $VerActual = $Resolved.Version
    $VerLocal = $Resolved.Local

    InfoMessage "The actual version of Python to use:"
    InfoMessage ("$VerActual " + $(if ($VerLocal) {"(local)"} else {"(remote)"}))

    if (!$VerLocal) {
        InfoMessage "Installing Python $VerActual, as the version is not installed locally."
        ExecCheckReturnCode "pyenv install $VerActual"
    }

    InfoMessage "Setting the version as a local version to use."
    ExecCheckReturnCode "pyenv local $VerActual"

    $VenvPath=".\$WheelsBuildDir\epy$Ver"

    InfoMessage "Creating and activating a virtual environment at $VenvPath."
	ExecCheckReturnCode "pyenv exec python --version"
	ExecCheckReturnCode "pyenv exec python -m venv $VenvPath"
	. "$VenvPath\Scripts\Activate.ps1"

    InfoMessage "Building package."
    ExecCheckReturnCode "pyenv exec pip install -e ."
	ExecCheckReturnCode "pyenv rehash"

    InfoMessage "Building wheel."
	ExecCheckReturnCode "pyenv exec pip install wheel"
	ExecCheckReturnCode "pyenv rehash"
	ExecCheckReturnCode "pyenv exec pip wheel . --no-deps -w $WheelsTargetDir"

    InfoMessage "Deactivating virtual environment."
    deactivate
}
