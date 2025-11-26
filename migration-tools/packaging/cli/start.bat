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

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.

@rem Resolve any "." and ".." in LIB_DIR to make it shorter.
for %%i in ("@LIB_DIR@") do set LIB_DIR=%%~fi

set APP_JAR=@APP_JAR@
set MAIN_CLASS=@MAIN_CLASS@
set ARGS=@ARGS@

call %LIB_DIR%\setup-java.bat
if %ERRORLEVEL% neq 0 goto mainEnd

call %LIB_DIR%\jvmdefaults.bat
if %ERRORLEVEL% neq 0 goto mainEnd

set LOG_DIR="@LOG_DIR@"
set CLASSPATH="%LIB_DIR%\%APP_JAR%;%LIB_DIR%\*"

set DEFAULT_JVM_OPTS=-Dfile.encoding=UTF-8 ^
-XX:+HeapDumpOnOutOfMemoryError ^
-XX:+ExitOnOutOfMemoryError ^
-XX:HeapDumpPath=%LOG_DIR%

"%JAVA_EXE%" %JAVA_OPEN_MODULES% %DEFAULT_JVM_OPTS% -classpath %CLASSPATH% %MAIN_CLASS% %ARGS%

:mainEnd
if "%OS%"=="Windows_NT" endlocal
