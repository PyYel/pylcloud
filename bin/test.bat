@echo off
setlocal enabledelayedexpansion

REM Enable ANSI color codes in Windows 10+
reg add HKCU\Console /v VirtualTerminalLevel /t REG_DWORD /d 1 /f >nul 2>&1

REM Define color codes using ESC character
for /F %%a in ('echo prompt $E ^| cmd') do set "ESC=%%a"
set "COLOR_RESET=%ESC%[0m"
set "COLOR_GREEN=%ESC%[92m"
set "COLOR_RED=%ESC%[91m"
set "COLOR_YELLOW=%ESC%[93m"
set "COLOR_BLUE=%ESC%[94m"
set "COLOR_CYAN=%ESC%[96m"
set "COLOR_MAGENTA=%ESC%[95m"

REM Navigate to project root from bin folder
cd /d %~dp0..
echo %COLOR_CYAN%Project root: %CD%%COLOR_RESET%

REM Activate virtual environment
echo:
echo %COLOR_BLUE%Activating virtual environment...%COLOR_RESET%
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo %COLOR_RED%ERROR: Failed to activate virtual environment%COLOR_RESET%
    pause
    exit /b 1
)
echo %COLOR_GREEN%Virtual environment activated successfully%COLOR_RESET%

REM Install package manager uv
echo:
echo %COLOR_BLUE%Installing package manager...%COLOR_RESET%
pip install uv --quiet
if errorlevel 1 (
    echo %COLOR_RED%ERROR: Failed to install uv%COLOR_RESET%
    pause
    exit /b 1
)
echo %COLOR_GREEN%Package manager installed successfully%COLOR_RESET%

REM Install package in editable mode
echo:
echo %COLOR_BLUE%Installing packages in editable mode...%COLOR_RESET%
uv pip install -e . --quiet
if errorlevel 1 (
    echo %COLOR_RED%ERROR: Failed to install package%COLOR_RESET%
    pause
    exit /b 1
)
echo %COLOR_GREEN%Packages installed successfully%COLOR_RESET%

REM Find and run all main.py files in test folders
echo.
echo %COLOR_MAGENTA%========================================%COLOR_RESET%
echo %COLOR_MAGENTA%Running all test main.py files...%COLOR_RESET%
echo %COLOR_MAGENTA%========================================%COLOR_RESET%

set TEST_COUNT=0
set FAILED_COUNT=0
set SUCCESS_COUNT=0

for /r "pylcloud" %%f in (main.py) do (
    set "filepath=%%f"
    REM Check if file is in a test folder
    echo !filepath! | findstr /i "\\test\\" >nul
    if not errorlevel 1 (
        set /a TEST_COUNT+=1
        echo:
        echo %COLOR_CYAN%----------------------------------------%COLOR_RESET%
        echo %COLOR_CYAN%[!TEST_COUNT!] Running: %%f%COLOR_RESET%
        echo %COLOR_CYAN%----------------------------------------%COLOR_RESET%
        python "%%f"
        if errorlevel 1 (
            set /a FAILED_COUNT+=1
            echo %COLOR_RED%[FAILED] %%f%COLOR_RESET%
        ) else (
            set /a SUCCESS_COUNT+=1
            echo %COLOR_GREEN%[SUCCESS] %%f%COLOR_RESET%
        )
    )
)

REM Summary
echo.
echo %COLOR_MAGENTA%========================================%COLOR_RESET%
echo %COLOR_MAGENTA%Test Summary%COLOR_RESET%
echo %COLOR_MAGENTA%========================================%COLOR_RESET%
echo %COLOR_CYAN%Total tests run: !TEST_COUNT!%COLOR_RESET%

if !FAILED_COUNT! GTR 0 (
    echo %COLOR_RED%Failed tests: !FAILED_COUNT!%COLOR_RESET%
) else (
    echo %COLOR_GREEN%Failed tests: 0%COLOR_RESET%
)

echo %COLOR_GREEN%Successful tests: !SUCCESS_COUNT!%COLOR_RESET%
echo %COLOR_MAGENTA%========================================%COLOR_RESET%

if !FAILED_COUNT! GTR 0 (
    echo.
    echo %COLOR_RED%Some tests failed. See the output above.%COLOR_RESET%
) else (
    echo.
    echo %COLOR_GREEN%All tests passed successfully.%COLOR_RESET%
)

pause