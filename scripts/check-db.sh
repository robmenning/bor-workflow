#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi
if [ -f .env.local ]; then
    export $(grep -v '^#' .env.local | xargs)
fi

# Check if PostgreSQL container is running
if ! docker ps | grep -q bor-workflow-db; then
    echo "PostgreSQL container is not running. Please start it first."
    exit 1
fi

# Connect to PostgreSQL and check tables
echo "Checking Prefect database tables..."
docker exec bor-workflow-db psql -U prefect -d prefect -c "
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'prefect' 
ORDER BY table_name;
"

# Check if any tables exist
if [ $? -eq 0 ]; then
    echo -e "\nDatabase is initialized and ready for use."
else
    echo -e "\nDatabase tables not found. They will be created when Prefect server starts."
fi 