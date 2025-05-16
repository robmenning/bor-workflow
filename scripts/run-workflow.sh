#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi
if [ -f .env.local ]; then
    export $(grep -v '^#' .env.local | xargs)
fi

# Set Prefect API URLs based on environment
if [ "$ENV" = "production" ]; then
    export PREFECT_API_URL="http://bor-workflow:4200/api"
    export PREFECT_UI_API_URL="http://localhost:4640/api"
else
    export PREFECT_API_URL="http://bor-workflow:4200/api"
    export PREFECT_UI_API_URL="http://localhost:4440/api"
fi

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Ensure containers are running
./scripts/manage-containers.sh start

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Run the workflow using the virtual environment's Python
python -c "
from src.workflows.file_ingestion import file_ingestion_workflow
file_ingestion_workflow(
    file_path='$1',
    ftp_host='bor-files',
    ftp_port=21,
    db_host='bor-db',
    db_port=3306,
    db_name='bormeta'
)
" 