#!/bin/bash


##!/bin/bash
## Define the file name
#FILE_NAME="example.txt"
## Create the text file
#touch "$FILE_NAME"
## Add some content to the file
#echo "This is a sample text file created by a bash script." > "$FILE_NAME"
## Print a message
#echo "File '$FILE_NAME' has been created."




## Define the CSV file path
#CSV_FILE="MOCK_DATA.csv"
## Check if the CSV file exists
#if [ ! -f "$CSV_FILE" ]; then
#  echo "CSV file does not exist: $CSV_FILE"
#  exit 1
#fi
## Print the first five lines of the CSV file
#echo "First five lines of $CSV_FILE:"
#head -n 5 "$CSV_FILE"




#!/bin/bash

# Define the CSV file path
CSV_FILE="MOCK_DATA.csv"

# Check if the CSV file exists
if [ ! -f "$CSV_FILE" ]; then
  echo "CSV file does not exist: $CSV_FILE"
  exit 1
fi

# Print the first five lines of the CSV file
echo "First five lines of $CSV_FILE:"
head -n 5 "$CSV_FILE"

# Define the output schema file name
SCHEMA_FILE="schema_${CSV_FILE%.*}.txt"

# Python script to read the schema and write to a text file
PYTHON_SCRIPT=$(cat <<EOF
import pandas as pd
import sys

input_csv = sys.argv[1]
output_txt = sys.argv[2]

# Read the CSV file
df = pd.read_csv(input_csv)

# Get the schema
schema = df.dtypes

# Write the schema to the output text file
with open(output_txt, 'w') as f:
    f.write(str(schema))
EOF
)

# Execute the Python script
python3 -c "$PYTHON_SCRIPT" "$CSV_FILE" "$SCHEMA_FILE"

if [ $? -eq 0 ]; then
  echo "Schema written to $SCHEMA_FILE"
else
  echo "Failed to write schema"
  exit 1
fi



