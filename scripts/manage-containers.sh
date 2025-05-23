#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 {start|stop|clear|status}"
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

# Function to start containers
start_containers() {
    echo "Starting containers..."
    
    # Start PostgreSQL database
    docker run -d --name bor-workflow-db \
        --network bor-network \
        -v bor-workflow-db-data:/var/lib/postgresql/data \
        -e POSTGRES_USER=prefect \
        -e POSTGRES_PASSWORD=prefect \
        -e POSTGRES_DB=prefect \
        postgres:latest

    # Wait for PostgreSQL to be ready
    if ! wait_for_postgres; then
        echo "Failed to start PostgreSQL. Stopping containers..."
        stop_containers
        exit 1
    fi

    # Initialize PostgreSQL
    init_postgres

    # Start Prefect server (do NOT start a worker here)
    docker run -d --name bor-workflow \
        --network bor-network \
        -p 4440:4200 \
        -e PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@bor-workflow-db:5432/prefect" \
        -e PREFECT_API_URL="http://bor-workflow:4200/api" \
        -e PREFECT_UI_API_URL="http://localhost:4440/api" \
        -e PREFECT_SERVER_API_HOST="0.0.0.0" \
        -e PREFECT_SERVER_API_PORT="4200" \
        -v $(pwd)/src:/opt/prefect/src \
        -v $(pwd)/prefect.yaml:/opt/prefect/prefect.yaml \
        -v bor-files-data:/var/lib/mysql-files \
        prefecthq/prefect:3-latest \
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
        -e PREFECT_API_URL="http://bor-workflow:4200/api" \
        -e PREFECT_AGENT_API_URL="http://bor-workflow:4200/api" \
        -v bor-files-data:/var/lib/mysql-files \
        bor-etl-agent:latest \
        prefect worker start --pool default-agent-pool

    # Create necessary directories in the shared volume
    docker exec bor-workflow mkdir -p /data/imports
    docker exec bor-workflow chmod 777 /data/imports

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
        start_containers
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