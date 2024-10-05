#!/bin/bash

# Load environment variables from .env file in the parent directory
set -a
source ../.env
set +a

# Table-specific and output-specific variables
TABLE_NAME="sales"
OUTPUT_FILE="sales_data.sql"

# Check if essential environment variables are set
if [ -z "$DB_NAME" ] || [ -z "$DB_USER" ] || [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_PASSWORD" ]; then
    echo "One or more required environment variables (DB_NAME, DB_USER, DB_HOST, DB_PORT, DB_PASSWORD) are missing."
    exit 1
fi

# Use PGPASSWORD to pass the password to pg_dump without exposing it in the command line
export PGPASSWORD=$DB_PASSWORD

# Exporting the data from sales_data table
pg_dump -U $DB_USER -h $DB_HOST -p $DB_PORT -t $TABLE_NAME --data-only --column-inserts --no-owner $DB_NAME > $OUTPUT_FILE

# Check if the export was successful
if [ $? -eq 0 ]; then
    echo "Data export successful. File saved as $OUTPUT_FILE"
else
    echo "Data export failed."
    exit 1
fi

# Unset the password environment variable for security
unset PGPASSWORD
