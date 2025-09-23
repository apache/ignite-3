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

if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto checkJdkVersion

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

exit /b %ERRORLEVEL%

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%\bin\java.exe

if exist "%JAVA_EXE%" goto checkJdkVersion

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

exit /b 1

:checkJdkVersion

for /f "tokens=* USEBACKQ" %%f in (`"%JAVA_EXE%" -version 2^>^&1`) do (
    set JAVA_FULL_VER_STR=%%f
    goto :LoopEscape
)
:LoopEscape

for /f "tokens=1-3  delims= " %%a in ("%JAVA_FULL_VER_STR%") do set JAVA_VER_STR=%%c
set JAVA_VER_STR=%JAVA_VER_STR:"=%

for /f "tokens=1,2 delims=." %%a in ("%JAVA_VER_STR%.x") do set JAVA_VER=%%a& set MINOR_JAVA_VER=%%b
if %JAVA_VER% == 1 set JAVA_VER=%MINOR_JAVA_VER%

if %JAVA_VER% LSS 11 (
  echo ERROR: java version should be equal or higher than 11, your current version is %JAVA_FULL_VER_STR% >&2
  exit /b 1
)
