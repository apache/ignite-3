@echo off

set ODBC_AMD64=%1

if [%ODBC_AMD64%] == [] (
	echo error: driver path is not specified. Call format: install_win abs_path_to_64_bit_driver
	pause
	exit /b 1
)

if exist %ODBC_AMD64% (
	for %%i IN (%ODBC_AMD64%) DO IF EXIST %%~si\NUL (
		echo warning: The path you have specified seems to be a directory. Note that you have to specify path to driver file itself instead.
	)
	echo Installing 64-bit driver: %ODBC_AMD64%
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v DriverODBCVer /t REG_SZ /d "03.80" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v UsageCount /t REG_DWORD /d 00000001 /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Driver /t REG_SZ /d %ODBC_AMD64% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Setup /t REG_SZ /d %ODBC_AMD64% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "Apache Ignite 3" /t REG_SZ /d "Installed" /f
) else (
	echo 64-bit driver can not be found: %ODBC_AMD64%
	echo Call format: install_win abs_path_to_64_bit_driver
	pause
	exit /b 1
)
