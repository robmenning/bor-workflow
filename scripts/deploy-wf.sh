#!/bin/bash

# Set the work pool name here for consistency
POOL="default-agent-pool"

# restart the containers
# ./scripts/manage-containers.sh stop; ./scripts/manage-containers.sh clear; ./scripts/manage-containers.sh start; docker ps

# Builds and configures the deployment
# ./scripts/deploy-wf.sh build    

# Start the agent
# ./scripts/deploy-wf.sh start-agent [background|new-terminal]

# Monitor the workflow in the Prefect UI at http://localhost:4440

# Build the deployment with default parameters (for scheduled runs) (remove, handled by the build step):
# docker exec bor-workflow prefect deploy src/workflows/file_ingestion.py:file_ingestion_workflow \
#   --name "File Ingestion" \
#   --pool "$POOL" \
#   --params '{"file_path": "/var/lib/mysql-files/fund_class_fee.csv", "db_user": "borETLSvc", "db_password": "u67nomZyNg"}'

# Run the workflow ad-hoc:
# docker exec bor-workflow prefect deployment run 'File Ingestion Workflow/File Ingestion' --params '{"file_path": "/var/lib/mysql-files/fund_class_fee.csv", "db_user": "borETLSvc", "db_password": "u67nomZyNg"}'


# Function to display usage
usage() {
    echo "Usage: $0 {build|start-agent [background|new-terminal]}"
    exit 1
}

# Function to build deployment
build_deployment() {
    echo "Building workflow deployment..."
    docker exec -it bor-workflow prefect deploy \
        src/workflows/file_ingestion.py:file_ingestion_workflow \
        --name "File Ingestion" \
        --pool "$POOL" \
        --params '{"file_path": "/var/lib/mysql-files/fund_class_fee.csv", "db_user": "borETLSvc", "db_password": "u67nomZyNg"}'
}

# Function to start agent
start_agent() {
    local mode=$1
    
    case "$mode" in
        "background")
            echo "Starting Prefect worker in background..."
            # Start the worker and redirect output to a log file
            docker exec -d bor-workflow bash -c "prefect worker start --pool '$POOL' > /var/log/prefect-worker.log 2>&1"
            echo "Worker started in background. Check logs with: docker exec bor-workflow cat /var/log/prefect-worker.log"
            ;;
        "new-terminal")
            if command -v gnome-terminal &> /dev/null; then
                gnome-terminal -- bash -c "docker exec -it bor-workflow prefect worker start --pool '$POOL'; exec bash"
            elif command -v xterm &> /dev/null; then
                xterm -e "docker exec -it bor-workflow prefect worker start --pool '$POOL'; exec bash" &
            else
                echo "No supported terminal emulator found. Running in current terminal..."
                docker exec -it bor-workflow prefect worker start \
                    --pool "$POOL"
            fi
            ;;
        *)
            echo "Starting Prefect worker in current terminal..."
            echo "Note: This will keep the terminal occupied. Use Ctrl+C to stop."
            docker exec -it bor-workflow prefect worker start \
                --pool "$POOL"
            ;;
    esac
}

# Main script logic
case "$1" in
    build)
        build_deployment
        ;;
    start-agent)
        start_agent "$2"
        ;;
    *)
        usage
        ;;
esac

exit 0 