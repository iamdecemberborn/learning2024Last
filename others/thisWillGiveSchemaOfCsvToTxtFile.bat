@echo off

REM Define Variables
SET SCRIPT_DIR=%~dp0
SET INPUT_CSV=%SCRIPT_DIR%MOCK_DATA.csv
SET OUTPUT_TXT=%SCRIPT_DIR%schema_output_%date:~10,4%%date:~4,2%%date:~7,2%%time:~0,2%%time:~3,2%%time:~6,2%.txt

REM Check if the input CSV file exists
IF NOT EXIST "%INPUT_CSV%" (
    ECHO Input CSV file does not exist: %INPUT_CSV%
    EXIT /B 1
)

REM Python script to read the schema and write to a text file
SET PYTHON_SCRIPT=%SCRIPT_DIR%read_schema.py
ECHO import pandas as pd > %PYTHON_SCRIPT%
ECHO import sys >> %PYTHON_SCRIPT%
ECHO. >> %PYTHON_SCRIPT%
ECHO input_csv = sys.argv[1] >> %PYTHON_SCRIPT%
ECHO output_txt = sys.argv[2] >> %PYTHON_SCRIPT%
ECHO. >> %PYTHON_SCRIPT%
ECHO df = pd.read_csv(input_csv) >> %PYTHON_SCRIPT%
ECHO schema = df.dtypes >> %PYTHON_SCRIPT%
ECHO. >> %PYTHON_SCRIPT%
ECHO with open(output_txt, 'w') as f: >> %PYTHON_SCRIPT%
ECHO ^    f.write(str(schema)) >> %PYTHON_SCRIPT%

REM Execute the Python script
python %PYTHON_SCRIPT% "%INPUT_CSV%" "%OUTPUT_TXT%"

IF %ERRORLEVEL% EQU 0 (
    ECHO Schema written to %OUTPUT_TXT%
) ELSE (
    ECHO Failed to write schema
    EXIT /B 1
)

REM Clean up
DEL %PYTHON_SCRIPT%
