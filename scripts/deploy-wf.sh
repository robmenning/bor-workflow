#!/bin/bash

# Usage: ./scripts/deploy-wf.sh build <workflow_name>
# Example: ./scripts/deploy-wf.sh build import_web_classfees

set -e

# after creating a new workflow:
# pip install -r requirements.txt
# python src/workflows/pdf_extraction.py


# restart the containers
    # ./scripts/manage-containers.sh stop; ./scripts/manage-containers.sh clear; ./scripts/manage-containers.sh start dev; docker ps

# deploy wf
    # ./scripts/deploy-wf.sh build import_web_classfees

# start agent
    # ./scripts/deploy-wf.sh start-agent [background|new-terminal]

# start worker in foreground to see logs
    # docker exec -it bor-workflow prefect worker start --pool default-agent-pool

# monitor wf
    # ./scripts/deploy-wf.sh status


# add a new workflow:
    # File: src/workflows/<workflow_name>.py
        #    from prefect import flow
        #    @flow
        #    def my_new_workflow():
        #        ...
    # Flow function: <workflow_name>_workflow
    # Deployment name: Human-readable, but derived from the workflow name.

    # register:
        # In src/workflows/__init__.py
        # from .my_new_workflow import my_new_workflow
        # register_workflow("my_new_workflow", my_new_workflow)

    # deploy:
        # ./scripts/deploy-wf.sh build my_new_workflow

    # or
    # docker exec -it bor-workflow prefect deploy --apply

# to copy a file into the volume accessible by bor-db: 
# docker cp tests/data/holdweb-20241231.csv bor-db:/var/lib/mysql-files/ftpetl/incoming/holdweb-20241231.csv

# deploy all workflows
# docker exec -it bor-workflow prefect deploy --all

# Set environment variables with defaults
export PREFECT_API_URL=${PREFECT_API_URL:-"http://bor-workflow:4200/api"}
# Set environment variables with defaults
export PREFECT_API_URL=${PREFECT_API_URL:-"http://bor-workflow:4200/api"}
export PREFECT_WORK_POOL=${PREFECT_WORK_POOL:-"default-agent-pool"}

# Function to display usage
usage() {
    echo "Usage: $0 {build|start-agent|deploy|list|status} [options]"
    echo "Commands:"
    echo "  build <workflow_name>    - Build a workflow deployment"
    echo "  start-agent [mode]       - Start the Prefect agent"
    echo "  deploy <workflow_name>   - Deploy a workflow"
    echo "  list                     - List all deployments"
    echo "  status                   - Show deployment status"
    echo ""
    echo "Environment variables:"
    echo "  PREFECT_API_URL         - Prefect API URL (default: http://bor-workflow:4200/api)"
    echo "  PREFECT_WORK_POOL       - Work pool name (default: default-agent-pool)"
    exit 1
}

# Function to build deployment
build_deployment() {
    echo "Building and applying deployment(s) from prefect.yaml using pool $PREFECT_WORK_POOL"
    docker exec -e PREFECT_API_URL -e PREFECT_WORK_POOL -it bor-workflow prefect deploy
}

# Function to start agent
start_agent() {
    local mode=$1
    
    echo "Starting agent with work pool: $PREFECT_WORK_POOL"
    case "$mode" in
        "background")
            echo "Starting Prefect worker in background..."
            docker exec -d bor-workflow bash -c "prefect worker start --pool '$PREFECT_WORK_POOL' > /var/log/prefect-worker.log 2>&1"
            echo "Worker started in background. Check logs with: docker exec bor-workflow cat /var/log/prefect-worker.log"
            ;;
        "new-terminal")
            if command -v gnome-terminal &> /dev/null; then
                gnome-terminal -- bash -c "docker exec -it bor-workflow prefect worker start --pool '$PREFECT_WORK_POOL'; exec bash"
            elif command -v xterm &> /dev/null; then
                xterm -e "docker exec -it bor-workflow prefect worker start --pool '$PREFECT_WORK_POOL'; exec bash" &
            else
                echo "No supported terminal emulator found. Running in current terminal..."
                docker exec -it bor-workflow prefect worker start --pool "$PREFECT_WORK_POOL"
            fi
            ;;
        *)
            echo "Starting Prefect worker in current terminal..."
            echo "Note: This will keep the terminal occupied. Use Ctrl+C to stop."
            docker exec -it bor-workflow prefect worker start --pool "$PREFECT_WORK_POOL"
            ;;
    esac
}

# Function to list deployments
list_deployments() {
    echo "Listing all deployments..."
    docker exec bor-workflow prefect deployment ls
}

# Function to show deployment status
show_status() {
    echo "Deployment status:"
    docker exec bor-workflow prefect deployment ls --json | jq '.[] | {name: .name, schedule: .schedule, tags: .tags}'
}

# Main script logic
case "$1" in
    build)
        build_deployment
        ;;
    start-agent)
        start_agent "$2"
        ;;
    list)
        list_deployments
        ;;
    status)
        show_status
        ;;
    *)
        usage
        ;;
esac

exit 0 