#!/bin/bash

# usage examples:
# ./scripts/manage-containers.sh start dev
# ./scripts/manage-containers.sh stop
# ./scripts/manage-containers.sh clear
# ./scripts/manage-containers.sh status

# Function to display usage
usage() {
    echo "Usage: $0 {start|stop|clear|status} [dev|prod] [--use-custom-image]"
    echo "  --use-custom-image: Use the custom Dockerfile instead of official Prefect image"
    exit 1
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    echo "Waiting for PostgreSQL to be ready..."
    for i in {1..30}; do
        if docker exec bor-workflow-db pg_isready -U prefect > /dev/null 2>&1; then
            echo "PostgreSQL is ready!"
            return 0
        fi
        echo "Waiting for PostgreSQL... ($i/30)"
        sleep 2
    done
    echo "PostgreSQL failed to start within 60 seconds"
    return 1
}

# Function to initialize PostgreSQL
init_postgres() {
    echo "Initializing PostgreSQL..."
    docker exec bor-workflow-db psql -U prefect -c "ALTER USER prefect WITH PASSWORD 'prefect';"
    docker exec bor-workflow-db psql -U prefect -c "GRANT ALL PRIVILEGES ON DATABASE prefect TO prefect;"
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

# Function to deploy flows with correct env vars using deployment build/apply
run_prefect_deploy_build() {
    local env_file="/tmp/merged.env"
    echo "Copying env file into bor-workflow container..."
    docker cp "$MERGED_ENV_FILE" bor-workflow:$env_file
    echo "Running prefect deploy to create deployments from prefect.yaml..."
    docker exec -i bor-workflow /bin/bash -c "export \
      \$(grep -v '^#' $env_file | xargs) && \
      cd /opt/prefect && \
      prefect deploy --name 'Import web classfees' --name 'Import web hold'"
}

# Function to start containers
start_containers() {
    local env=${1:-prod}
    local use_custom_image=${2:-false}
    echo "Starting containers for environment: $env..."
    merge_env_files "$env"
    local env_file="$MERGED_ENV_FILE"
    # Source the merged env file to get PREFECT_UI_PORT
    if [ -f "$env_file" ]; then
        set -a
        source "$env_file"
        set +a
        echo "DEBUG: PREFECT_UI_PORT is '$PREFECT_UI_PORT'"
    fi
    local ui_port="${PREFECT_UI_PORT:-4440}"
    
    # Start PostgreSQL database
    docker run -d --name bor-workflow-db \
        --network bor-network \
        --env-file "$env_file" \
        -v bor-workflow-db-data:/var/lib/postgresql/data \
        -e POSTGRES_USER=prefect \
        -e POSTGRES_PASSWORD=prefect \
        -e POSTGRES_DB=prefect \
        postgres:latest

    # Wait for PostgreSQL to be ready
    if ! wait_for_postgres; then
        echo "Failed to start PostgreSQL. Stopping containers..."
        stop_containers
        cleanup_env_file
        exit 1
    fi

    # Initialize PostgreSQL
    init_postgres

    # Build custom image if requested
    local prefect_image="prefecthq/prefect:3-latest"
    local volume_mounts="-v $(pwd)/src:/opt/prefect/src -v $(pwd)/prefect.yaml:/opt/prefect/prefect.yaml"
    
    if [ "$use_custom_image" = "true" ]; then
        echo "Building custom Prefect image..."
        docker build -t bor-workflow:latest .
        prefect_image="bor-workflow:latest"
        volume_mounts=""  # No volume mounts needed since code is baked into image
    fi

    # Start Prefect server (do NOT start a worker here)
    docker run -d --name bor-workflow \
        --network bor-network \
        --env-file "$env_file" \
        -p ${ui_port}:4200 \
        -e PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@bor-workflow-db:5432/prefect" \
        -e PREFECT_API_URL="http://bor-workflow:4200/api" \
        -e PREFECT_SERVER_API_HOST="0.0.0.0" \
        -e PREFECT_SERVER_API_PORT="4200" \
        $volume_mounts \
        -v bor-files-data:/var/lib/mysql-files \
        $prefect_image \
        prefect server start --host 0.0.0.0

    # Wait for Prefect server to be ready
    echo "Waiting for Prefect server to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:4440/api/health > /dev/null; then
            echo "Prefect server is ready!"
            break
        fi
        echo "Waiting for Prefect server... ($i/30)"
        sleep 2
    done

    # Build and start ETL agent (this is the ONLY worker)
    echo "Building ETL agent..."
    docker build -t bor-etl-agent -f docker/Dockerfile.etl-agent .
    echo "Starting ETL agent..."
    docker run -d --rm \
        --name bor-etl-agent \
        --network bor-network \
        --env-file "$env_file" \
        -e PREFECT_API_URL="http://bor-workflow:4200/api" \
        -e PREFECT_AGENT_API_URL="http://bor-workflow:4200/api" \
        -v bor-files-data:/var/lib/mysql-files \
        bor-etl-agent:latest \
        prefect worker start --pool default-agent-pool

    # Create necessary directories in the shared volume
    docker exec bor-workflow mkdir -p /data/imports
    docker exec bor-workflow chmod 777 /data/imports

    run_prefect_deploy_build
    cleanup_env_file
    echo "All containers started successfully!"
}

# Function to stop containers
stop_containers() {
    echo "Stopping containers..."
    docker stop bor-workflow bor-workflow-db bor-etl-agent
}

# Function to clear containers
clear_containers() {
    echo "Clearing containers..."
    docker rm -f bor-workflow bor-workflow-db bor-etl-agent
}

# Function to show container status
show_status() {
    echo "Container status:"
    docker ps -a --filter "name=bor-workflow"
}

# Main script logic
case "$1" in
    start)
        # Check for --use-custom-image flag
        local use_custom_image="false"
        for arg in "$@"; do
            if [ "$arg" = "--use-custom-image" ]; then
                use_custom_image="true"
                break
            fi
        done
        start_containers "$2" "$use_custom_image"
        ;;
    stop)
        stop_containers
        ;;
    clear)
        clear_containers
        ;;
    status)
        show_status
        ;;
    *)
        usage
        ;;
esac

exit 0 