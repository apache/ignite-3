@rem
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements. See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License. You may obtain a copy of the License at
@rem
@rem      http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  ignite3 startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and IGNITE3_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:execute

@rem if IGNITE_HOME is not set than it will be parent directory for bin
if "%IGNITE_HOME%"=="" set IGNITE_HOME=%APP_HOME%
cd "%IGNITE_HOME%"

call "@CONF_DIR@\@VARS_FILE_NAME@"

@rem Save base values before any overrides
set BASE_NODE_NAME=%NODE_NAME%
set BASE_WORK_DIR=%WORK_DIR%
set BASE_LOG_DIR=%LOG_DIR%
set BASE_CONFIG_FILE=%CONFIG_FILE%

@rem Initialize CLI override variables
set CLI_NODE_NAME=
set CLI_WORK_DIR=
set CLI_LOG_DIR=
set CLI_CONFIG_FILE=
set CLI_EXTRA_CLASSPATH=

@rem Parse command line arguments into CLI_* variables
:parseArgs
if "%~1"=="" goto applyOverrides
if "%~1"=="--node-name" goto argNodeName
if "%~1"=="--work-dir" goto argWorkDir
if "%~1"=="--log-dir" goto argLogDir
if "%~1"=="--config" goto argConfig
if "%~1"=="--extra-classpath" goto argExtraClasspath
shift
goto parseArgs

:argNodeName
if "%~2"=="" (echo Error: --node-name requires a value 1>&2 & goto fail)
set CLI_NODE_NAME=%~2
shift
shift
goto parseArgs

:argWorkDir
if "%~2"=="" (echo Error: --work-dir requires a value 1>&2 & goto fail)
set CLI_WORK_DIR=%~2
shift
shift
goto parseArgs

:argLogDir
if "%~2"=="" (echo Error: --log-dir requires a value 1>&2 & goto fail)
set CLI_LOG_DIR=%~2
shift
shift
goto parseArgs

:argConfig
if "%~2"=="" (echo Error: --config requires a value 1>&2 & goto fail)
set CLI_CONFIG_FILE=%~2
shift
shift
goto parseArgs

:argExtraClasspath
if "%~2"=="" (echo Error: --extra-classpath requires a value 1>&2 & goto fail)
set CLI_EXTRA_CLASSPATH=%~2
shift
shift
goto parseArgs

@rem Apply overrides in priority order: CLI args > IGNITE3_* env vars > defaults.
@rem When node name is overridden, work/log dirs become subdirectories and config file
@rem gets a node name suffix. Explicit path overrides take precedence over derived values.
:applyOverrides

@rem Resolve node name: CLI > env var > default
if defined CLI_NODE_NAME set NODE_NAME=%CLI_NODE_NAME%
if not defined CLI_NODE_NAME if defined IGNITE3_NODE_NAME set NODE_NAME=%IGNITE3_NODE_NAME%

@rem If node name differs from default, derive subdirectory paths
if not "%NODE_NAME%"=="%BASE_NODE_NAME%" (
    set "WORK_DIR=%BASE_WORK_DIR%\%NODE_NAME%"
    set "LOG_DIR=%BASE_LOG_DIR%\%NODE_NAME%"
    call set "CONFIG_FILE=%%BASE_CONFIG_FILE:.conf=-%NODE_NAME%.conf%%"
)

@rem Explicit path overrides take precedence over node-name derived values
if defined CLI_WORK_DIR set WORK_DIR=%CLI_WORK_DIR%
if not defined CLI_WORK_DIR if defined IGNITE3_WORK_DIR set WORK_DIR=%IGNITE3_WORK_DIR%

if defined CLI_LOG_DIR set LOG_DIR=%CLI_LOG_DIR%
if not defined CLI_LOG_DIR if defined IGNITE3_LOG_DIR set LOG_DIR=%IGNITE3_LOG_DIR%

if defined CLI_CONFIG_FILE set CONFIG_FILE=%CLI_CONFIG_FILE%
if not defined CLI_CONFIG_FILE if defined IGNITE3_CONFIG_FILE set CONFIG_FILE=%IGNITE3_CONFIG_FILE%

@rem Extra classpath: CLI > env var
if defined CLI_EXTRA_CLASSPATH set IGNITE3_EXTRA_CLASSPATH=%CLI_EXTRA_CLASSPATH%

@rem Create work and log directories if they do not exist
if not exist "%WORK_DIR%" mkdir "%WORK_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

call "%LIBS_DIR%\@BOOTSTRAP_FILE_NAME@"

@rem Execute ignite3
%JAVA_CMD_WITH_ARGS% %APPLICATION_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if %ERRORLEVEL% equ 0 goto mainEnd

:fail
rem Set variable IGNITE3_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% equ 0 set EXIT_CODE=1
if not ""=="%IGNITE3_EXIT_CONSOLE%" exit %EXIT_CODE%
exit /b %EXIT_CODE%

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
