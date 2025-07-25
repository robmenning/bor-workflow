#!/bin/bash

# Script to deploy Prefect workflows manually
# Usage: ./scripts/deploy-wf.sh [dev|prod]

# Function to display usage
usage() {
    echo "Usage: $0 [dev|prod]"
    echo "  Deploys all workflows defined in prefect.yaml"
    exit 1
}

# Function to merge env files for the current environment
merge_env_files() {
    local env=${1:-prod}
    local env_file=".env"
    local env_main=".env.production"
    local env_local=".env.production.local"
    local merged_file=".env.production.merged"
    if [ "$env" = "dev" ]; then
        env_main=".env.development"
        env_local=".env.development.local"
        merged_file=".env.development.merged"
    fi
    echo "Merging $env_file, $env_main, and $env_local into $merged_file..."
    > "$merged_file"
    if [ -f "$env_file" ]; then
        cat "$env_file" >> "$merged_file"
    fi
    if [ -f "$env_main" ]; then
        cat "$env_main" >> "$merged_file"
    fi
    if [ -f "$env_local" ]; then
        cat "$env_local" >> "$merged_file"
    fi
    if [ ! -s "$merged_file" ]; then
        echo "No env files found to merge!"
        exit 1
    fi
    export MERGED_ENV_FILE="$merged_file"
}

# Function to clean up merged env file
cleanup_env_file() {
    if [ -n "$MERGED_ENV_FILE" ] && [ -f "$MERGED_ENV_FILE" ]; then
        rm "$MERGED_ENV_FILE"
        echo "Cleaned up $MERGED_ENV_FILE"
    fi
}

# Function to deploy flows
deploy_flows() {
    local env=${1:-prod}
    echo "Deploying workflows for environment: $env..."
    
    # Check if containers are running
    if ! docker ps | grep -q bor-workflow; then
        echo "Error: bor-workflow container is not running!"
        echo "Please start containers first with: ./scripts/manage-containers.sh start $env"
        exit 1
    fi
    
    merge_env_files "$env"
    local env_file="/tmp/merged.env"
    
    echo "Copying env file into bor-workflow container..."
    docker cp "$MERGED_ENV_FILE" bor-workflow:$env_file
    
    echo "Deploying workflows from prefect.yaml..."
    docker exec -i bor-workflow /bin/bash -c "export \
      \$(grep -v '^#' $env_file | xargs) && \
      prefect deploy --all"
    
    cleanup_env_file
    echo "Workflow deployment completed!"
}

# Main script logic
case "$1" in
    dev|prod)
        deploy_flows "$1"
        ;;
    "")
        deploy_flows "prod"
        ;;
    *)
        usage
        ;;
esac

exit 0 