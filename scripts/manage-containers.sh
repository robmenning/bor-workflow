#!/bin/bash


# This script is used to manage the containers for the BOR workflow.
# It can start, stop, restart, show status, show logs, or clean up the containers and volumes.
# The script also checks if the database is initialized and waits for it to be initialized if it is not.
# command line arguments:
# start: start the containers
# stop: stop the containers
# restart: restart the containers
# status: show the status of the containers
# logs: show the logs of the containers
# clean: clean up the containers and volumes

# usage: 
# ./manage-containers.sh start|stop|restart|status|logs|clean
# usage with setting ENV first
# ENV=development ./manage-containers.sh start


# Default to development environment if not specified
ENV=${ENV:-development}

# Load base environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Load environment-specific variables
if [ "$ENV" = "development" ]; then
    if [ -f .env.development.local ]; then
        export $(grep -v '^#' .env.development.local | xargs)
    elif [ -f .env.development ]; then
        export $(grep -v '^#' .env.development | xargs)
    fi
elif [ "$ENV" = "production" ]; then
    if [ -f .env.production.local ]; then
        export $(grep -v '^#' .env.production.local | xargs)
    elif [ -f .env.production ]; then
        export $(grep -v '^#' .env.production | xargs)
    fi
else
    echo "Invalid environment specified. Use 'development' or 'production'."
    exit 1
fi

# Set default port mapping for bor-workflow (dev: 4440, prod: 4640)
if [ "$ENV" = "production" ]; then
    WORKFLOW_HOST_PORT=4640
else
    WORKFLOW_HOST_PORT=4440
fi

# Function to check if a container exists
container_exists() {
    docker ps -a --format '{{.Names}}' | grep -q "^$1$"
}

# Function to check if a container is running
container_running() {
    docker ps --format '{{.Names}}' | grep -q "^$1$"
}

# Function to stop and remove a container
stop_and_remove() {
    if container_exists $1; then
        echo "Stopping and removing $1..."
        docker stop $1
        docker rm $1
    fi
}

# Function to build and start a container
build_and_start() {
    local container_name=$1
    local dockerfile=$2
    local port=$3
    shift 3
    local volumes=("$@")

    echo "Building $container_name..."
    docker build -t $container_name:dev -f $dockerfile .

    echo "Starting $container_name..."
    local volume_args=""
    for volume in "${volumes[@]}"; do
        volume_args="$volume_args -v $volume"
    done

    docker run -d \
        --name $container_name \
        --network bor-network \
        -p $port \
        $volume_args \
        $container_name:dev
}

# Function to check database initialization
check_db_init() {
    echo "Checking database initialization..."
    docker exec bor-workflow-db psql -U prefect -d prefect -c "
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'prefect' 
        AND table_name = 'flow_run'
    );" | grep -q "t"
    return $?
}

build_and_start_agent() {
    echo "Building bor-etl-agent..."
    docker build -t bor-etl-agent:latest -f Dockerfile.etl-agent .
    echo "Starting bor-etl-agent..."
    docker run -d \
        --name bor-etl-agent \
        --network bor-network \
        -e PREFECT_API_URL="http://bor-workflow:4200/api" \
        -e PREFECT_UI_API_URL="http://localhost:${WORKFLOW_HOST_PORT}/api" \
        bor-etl-agent:latest \
        prefect worker start --pool 'default-agent-pool'
}

case "$1" in
    "start")
        stop_and_remove "bor-workflow"
        stop_and_remove "bor-workflow-db"
        stop_and_remove "bor-etl-agent"

        # Start DB (no host port exposed)
        if ! container_running "bor-workflow-db"; then
            docker run -d --name bor-workflow-db \
                --network bor-network \
                -v bor-workflow-db-data:/var/lib/postgresql/data \
                -e POSTGRES_USER="${POSTGRES_USER}" \
                -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
                -e POSTGRES_DB="${POSTGRES_DB}" \
                postgres:latest
            echo "Waiting for PostgreSQL to start..."
            sleep 10
        fi

        # Start Prefect server (official image, mapped port based on ENV)
        if ! container_running "bor-workflow"; then
            docker run -d --rm \
                --name bor-workflow \
                --network bor-network \
                -p $WORKFLOW_HOST_PORT:4200 \
                -e PREFECT_API_DATABASE_CONNECTION_URL="${PREFECT_API_DATABASE_CONNECTION_URL}" \
                -e PREFECT_API_URL="http://bor-workflow:4200/api" \
                -e PREFECT_UI_API_URL="http://localhost:${WORKFLOW_HOST_PORT}/api" \
                prefecthq/prefect:3-latest \
                prefect server start --host 0.0.0.0
            echo "Waiting for Prefect server to start..."
            sleep 10
        fi

        # Start ETL agent (custom image, no host port exposed)
        if ! container_running "bor-etl-agent"; then
            build_and_start_agent
        fi
        ;;

    "stop")
        stop_and_remove "bor-workflow"
        stop_and_remove "bor-workflow-db"
        stop_and_remove "bor-etl-agent"
        ;;

    "status")
        for container in "bor-workflow-db" "bor-workflow" "bor-etl-agent"; do
            if container_running $container; then
                echo "$container: Running"
            elif container_exists $container; then
                echo "$container: Stopped"
            else
                echo "$container: Not created"
            fi
        done
        ;;

    "logs")
        if [ -z "$2" ]; then
            for container in "bor-workflow-db" "bor-workflow" "bor-etl-agent"; do
                docker logs $container
            done
        else
            docker logs $2
        fi
        ;;

    "clean")
        $0 stop
        echo "Removing volumes..."
        docker volume rm bor-workflow-db-data
        ;;

    *)
        echo "Usage: $0 {start|stop|status|logs|clean}"
        exit 1
        ;;
esac 