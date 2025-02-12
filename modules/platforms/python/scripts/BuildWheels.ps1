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
    For the script to work you should have Python distributions of the specified version installed on your machine
    to LOCALAPPDATA\Programs\Python\Python<Ver>, which is a default installation path.
.PARAMETER PyVers
    Python versions to use, through coma.
.EXAMPLE
    PS> .\scripts\BuildWheels.ps1 -PyVers "39,310,311,312,313"
    Build wheels for Python versions 3.9-3.13
.EXAMPLE
    PS> .\scripts\BuildWheels.ps1 -PyVers 311
    Build wheels for Python versions 3.11 only
#>

param ([Parameter(Mandatory)][string]$PyVers)

$ErrorActionPreference = "Stop"
$PyVersList=$PyVers.Replace(".","").Split(",") | ForEach-Object { $_.Trim() }
$WheelsBuildDir="wheels_build"
$WheelsTargetDir="distr"

foreach ($Ver in $PyVersList) {
    $PythonPath="$env:LOCALAPPDATA\Programs\Python\Python$Ver\python.exe"
    $VenvPath=".\$WheelsBuildDir\epy$Ver\Scripts\Activate.ps1"

    Write-Host ">>>" -ForegroundColor DarkGreen
    Write-Host ">>> Making a wheel for Python $Ver. (Path: $PythonPath)" -ForegroundColor DarkGreen
    Write-Host ">>>" -ForegroundColor DarkGreen

	& "$PythonPath" -m venv $WheelsBuildDir\epy$Ver
	. "$VenvPath"

	pip install -e .
	pip install wheel
	pip wheel . --no-deps -w $WheelsTargetDir
}
