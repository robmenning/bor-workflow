#!/bin/bash

# Usage:
#   ./scripts/deploy-wf.sh <file.py:flow> [<file.py> <flow>]
# Examples:
#   ./scripts/deploy-wf.sh src/workflows/file_ingestion.py:file_ingestion_workflow
#   ./scripts/deploy-wf.sh src/workflows/file_ingestion.py file_ingestion_workflow

if [ $# -eq 1 ]; then
    FLOW_PATH="$1"
elif [ $# -eq 2 ]; then
    FLOW_PATH="$1:$2"
else
    echo "Usage: $0 <file.py:flow> OR $0 <file.py> <flow>"
    exit 1
fi

# Extract the workflow file path and flow name
if [[ $FLOW_PATH == *":"* ]]; then
    WORKFLOW_FILE="${FLOW_PATH%%:*}"
    FLOW_NAME="${FLOW_PATH##*:}"
else
    WORKFLOW_FILE="$1"
    FLOW_NAME="$2"
fi

# Validate flow name doesn't contain unexpected characters
if [[ ! $FLOW_NAME =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Error: Invalid flow name '$FLOW_NAME'. Flow name should only contain letters, numbers, and underscores."
    exit 1
fi

echo "Deploying flow: $WORKFLOW_FILE:$FLOW_NAME to work pool 'default-agent-pool'..."

# Create a temporary directory in the container
docker exec -it bor-etl-agent mkdir -p /tmp/workflow

# Get the base filename
WORKFLOW_BASENAME=$(basename "$WORKFLOW_FILE")

# Copy both scripts to the container
docker cp scripts/deploy_flow.py bor-etl-agent:/tmp/workflow/
docker cp "$WORKFLOW_FILE" bor-etl-agent:/tmp/workflow/

# Run the deployment script with the correct environment
docker exec -it bor-etl-agent bash -c "cd /tmp/workflow && \
    PYTHONPATH=/tmp/workflow \
    # Unset any existing environment variables
    unset PREFECT_API_URL && \
    unset PREFECT_UI_API_URL && \
    # Set Prefect configuration
    prefect config set PREFECT_API_URL=http://bor-workflow:4200/api && \
    prefect config set PREFECT_UI_API_URL=http://localhost:4440/api && \
    # Create work pool if it doesn't exist
    prefect work-pool create default-agent-pool --type process || true && \
    # Run the deployment
    python /tmp/workflow/deploy_flow.py \"/tmp/workflow/$WORKFLOW_BASENAME:$FLOW_NAME\""