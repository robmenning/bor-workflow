#!/bin/bash
# Script to start MySQL container for the bor-db project
# This script handles initialization of MySQL with proper database setup
#
# USAGE:
#   ./script/db-start.sh [options]
#
# OPTIONS:
#   --env <mode>          Specify the environment mode:
#                         - production: Uses .env.production and .env.production.local
#                         - development: Uses .env.development and .env.development.local
#                         Defaults to development if not specified
#
#   --resetall, --reset   Completely resets the database environment by:
#                         - Removing existing volumes (bor-volume, bor-tablespaces)
#                         - Creating fresh volumes
#                         - Running full initialization with all DDL/DML scripts
#                         USE ONLY IN DEVELOPMENT - WILL DESTROY ALL DATA
#
#   --skip-init           Starts the container without running initialization scripts.
#                         Use this to restart an existing container without modifying data.
#                         This preserves existing database contents.
#
# ENVIRONMENT MODES:
#   DEVELOPMENT:          clear && ./script/db-stop.sh && ./script/db-start.sh --env development
#   PRODUCTION:           ./script/db-stop.sh && ./script/db-start.sh --env production
#                         NEVER use --resetall in production as it will DESTROY ALL DATA
#
# ENVIRONMENT FILES:
#   The script loads environment variables in this order:
#   1. .env (base variables)
#   2. For production mode:
#      - .env.production
#      - .env.production.local (overrides .env.production)
#   3. For development mode:
#      - .env.development
#      - .env.development.local (overrides .env.development)
#
#   Variables in later files override those in earlier files.
#   Required variables:
#   - DB_PORT: Port for MySQL (defaults to 4420 for dev, should be 4620 for prod)
#   - MYSQL_ROOT_PASSWORD: Root password for MySQL

# Process command line arguments
RESET_ALL=false
INITIALIZE=true
ENV_MODE="development"  # Default to development

while [ "$1" != "" ]; do
  case $1 in
    --resetall | --reset )  
      RESET_ALL=true
      ;;
    --skip-init )           
      INITIALIZE=false
      ;;
    --env )
      shift
      ENV_MODE=$1
      ;;
    * )
      echo "Unknown parameter: $1"
      echo "Valid parameters: --resetall, --reset, --skip-init, --env <mode>"
      exit 1
      ;;
  esac
  shift
done

# Load environment variables based on mode
echo "Loading environment for $ENV_MODE mode..."

# First load base .env if it exists
if [ -f .env ]; then
  source .env
fi

# Then load environment-specific files in order of precedence
case $ENV_MODE in
  "production")
    if [ -f .env.production ]; then
      source .env.production
    fi
    if [ -f .env.production.local ]; then
      source .env.production.local
    fi
    ;;
  "development")
    if [ -f .env.development ]; then
      source .env.development
    fi
    if [ -f .env.development.local ]; then
      source .env.development.local
    fi
    ;;
  *)
    echo "Invalid environment mode: $ENV_MODE"
    echo "Valid modes: production, development"
    exit 1
    ;;
esac

# Set default values if not in environment
DB_PORT=${DB_PORT:-4420}
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-defaultpassword} # Make sure to set this in your .env file

echo "====================== BOR DB Startup ======================"
echo "Starting MySQL container for BOR application"
echo "$(date)"

# Step 1: Clean up existing environment
echo "Stopping and removing existing container..."
docker stop bor-db 2>/dev/null || true
docker rm bor-db 2>/dev/null || true

# Step 2: Handle volume cleanup if requested
if [ "$RESET_ALL" = true ]; then
  echo "Resetting volumes for clean initialization..."
  docker volume rm bor-volume 2>/dev/null || true
  docker volume rm bor-tablespaces 2>/dev/null || true
fi

# Step 3: Ensure volumes exist
echo "Setting up Docker volumes..."
docker volume create bor-volume 2>/dev/null || true
docker volume create bor-tablespaces 2>/dev/null || true
docker volume create bor-files-data 2>/dev/null || true  # Shared volume for ETL files

# Step 3.5: Ensure proper permissions on volumes
echo "Setting proper permissions on volumes..."
# Create a temporary container to set permissions on the tablespaces volume
docker run --rm -v bor-tablespaces:/tablespaces alpine sh -c "mkdir -p /tablespaces && chmod 777 /tablespaces"

# Set permissions and create directory structure for ETL files volume
echo "Setting up ETL files volume structure..."
docker run --rm -v bor-files-data:/etl-files alpine sh -c "
  mkdir -p /etl-files/ftpetl/incoming /etl-files/ftpetl/processed /etl-files/ftpetl/archive && 
  chmod -R 777 /etl-files
"
echo "✓ Permissions and directory structure set"

# Step 4: Create backup directories
echo "Setting up backup directories..."
mkdir -p $(pwd)/script/backups/{bor,borinst,borarch,bormeta}

# Step 5: Prepare initialization if needed
if [ "$INITIALIZE" = true ]; then
  echo "Preparing initialization scripts..."
  
  # Create a temporary directory for initialization scripts
  TEMP_INIT_DIR=$(mktemp -d)
  echo "Created temporary directory: $TEMP_INIT_DIR"
  
  # Copy all initialization scripts to the temp directory
  cp $(pwd)/script/init/* "$TEMP_INIT_DIR/"
  
  # Make shell scripts executable
  chmod +x "$TEMP_INIT_DIR/"*.sh 2>/dev/null || true
  
  # Make sure scripts are readable by MySQL
  chmod -R 755 "$TEMP_INIT_DIR"
  
  echo "Initialization scripts prepared"
else
  echo "Skipping initialization setup (--skip-init flag used)"
fi

# Step 5.5: Build custom MySQL image with mysqlimport
echo "Building custom MySQL image with mysqlimport..."
docker build -t bor-mysql:8.0 -f Dockerfile .
echo "✓ Custom image built"

# Step 6: Start the MySQL container
echo "Starting MySQL container..."

# Create docker network if it doesn't exist
docker network inspect bor-network >/dev/null 2>&1 || docker network create bor-network

if [ "$INITIALIZE" = true ]; then
  # Start with initialization
  docker run --name bor-db \
    --network bor-network \
    -p ${DB_PORT}:3306 \
    -e MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}" \
    -e MYSQL_DATABASE=bor \
    -v bor-volume:/var/lib/mysql \
    -v bor-tablespaces:/tablespaces \
    -v bor-files-data:/var/lib/mysql-files \
    -v "$TEMP_INIT_DIR":/docker-entrypoint-initdb.d \
    -v "$(pwd)/script/backups/bor":/backups/bor \
    -v "$(pwd)/script/backups/borinst":/backups/borinst \
    -v "$(pwd)/script/backups/borarch":/backups/borarch \
    -v "$(pwd)/script/backups/bormeta":/backups/bormeta \
    -v "$(pwd)/script/config/my.cnf":/etc/mysql/conf.d/my.cnf \
    -d bor-mysql:8.0
else
  # Start without initialization
  docker run --name bor-db \
    --network bor-network \
    -p ${DB_PORT}:3306 \
    -e MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}" \
    -v bor-volume:/var/lib/mysql \
    -v bor-tablespaces:/tablespaces \
    -v bor-files-data:/var/lib/mysql-files \
    -v "$(pwd)/script/backups/bor":/backups/bor \
    -v "$(pwd)/script/backups/borinst":/backups/borinst \
    -v "$(pwd)/script/backups/borarch":/backups/borarch \
    -v "$(pwd)/script/backups/bormeta":/backups/bormeta \
    -v "$(pwd)/script/config/my.cnf":/etc/mysql/conf.d/my.cnf \
    -d bor-mysql:8.0
fi


# Step 7: Wait for MySQL to initialize
echo "Waiting for MySQL to initialize (40 seconds)... THIS IS IMPORTANT 10 SECONDS ISNT ENOUGH"
sleep 40

# Clean up temporary directory
if [ "$INITIALIZE" = true ]; then
  rm -rf "$TEMP_INIT_DIR"
  echo "Removed temporary initialization directory"
fi

# Step 8: Verify container is running properly
echo "Verifying container status..."
CONTAINER_RUNNING=$(docker ps -q -f "name=bor-db")

if [ -z "$CONTAINER_RUNNING" ]; then
  echo "WARNING: Container is not running. Check the logs:"
  docker logs bor-db | tail -50
  
  echo ""
  echo "Debug information:"
  echo "Container state: $(docker container inspect bor-db --format '{{.State.Status}}' 2>/dev/null || echo "Container doesn't exist")"
  echo "Volume: $(docker volume ls | grep bor-volume)"
  echo "Environment: MYSQL_ROOT_PASSWORD is ${MYSQL_ROOT_PASSWORD:+set}"
  
  echo ""
  echo "Suggestions:"
  echo "1. Check Docker logs with: docker logs bor-db"
  echo "2. Try running with --resetall flag"
  echo "3. Verify your environment variables in .env file"
  echo "4. Make sure port ${DB_PORT} is not in use"
  
  exit 1
else
  echo "Container is running successfully"
  
  # Only verify databases if we performed initialization
  if [ "$INITIALIZE" = true ]; then
    echo "Verifying database initialization..."
    
    # Check if all expected databases exist
    DBS_EXIST=$(docker exec bor-db mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "SHOW DATABASES;" 2>/dev/null | grep -E 'bor|borinst|borarch|bormeta' | wc -l)
    
    if [ "$DBS_EXIST" -lt 4 ]; then
      echo "WARNING: Not all expected databases were created. Found $DBS_EXIST of 4 expected databases."
      echo "Databases available:"
      docker exec bor-db mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "SHOW DATABASES;" 2>/dev/null
    else
      echo "All expected databases created successfully."
      
      # Check tablespaces were created properly
      echo "Checking tablespaces..."
      docker exec bor-db mysql -u root -p"${MYSQL_ROOT_PASSWORD}" \
        -e "SELECT * FROM information_schema.INNODB_TABLESPACES WHERE NAME IN ('ts_bor', 'ts_borinst', 'ts_borarch', 'ts_bormeta');" 2>/dev/null
      
      # Check table counts in each database
      for db in bor borinst borarch bormeta; do
        TABLE_COUNT=$(docker exec bor-db mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "USE $db; SHOW TABLES;" 2>/dev/null | wc -l)
        echo "Database $db has $((TABLE_COUNT-1)) tables."
      done
    fi
  else
    echo "Database initialization was skipped. Using existing data."
  fi
fi

# Step 9: Create backup scripts for each database
echo "Creating backup scripts..."

# bor database backup script
cat > script/backup-bor.sh << 'EOL'
#!/bin/bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker exec bor-db mysqldump -u borAdmin -pAye3aBYrXF --databases bor > ./script/backups/bor/bor_$TIMESTAMP.sql
echo "Backup of bor database created at ./script/backups/bor/bor_$TIMESTAMP.sql"
EOL

# borinst database backup script
cat > script/backup-borinst.sh << 'EOL'
#!/bin/bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker exec bor-db mysqldump -u borinstAdmin -pkBu9pjz2vi --databases borinst > ./script/backups/borinst/borinst_$TIMESTAMP.sql
echo "Backup of borinst database created at ./script/backups/borinst/borinst_$TIMESTAMP.sql"
EOL

# borarch database backup script
cat > script/backup-borarch.sh << 'EOL'
#!/bin/bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker exec bor-db mysqldump -u borarchAdmin -pkBu9pjz2vi --databases borarch > ./script/backups/borarch/borarch_$TIMESTAMP.sql
echo "Backup of borarch database created at ./script/backups/borarch/borarch_$TIMESTAMP.sql"
EOL

# bormeta database backup script
cat > script/backup-bormeta.sh << 'EOL'
#!/bin/bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker exec bor-db mysqldump -u bormetaAdmin -pkBu9pjz2vi --databases bormeta > ./script/backups/bormeta/bormeta_$TIMESTAMP.sql
echo "Backup of bormeta database created at ./script/backups/bormeta/bormeta_$TIMESTAMP.sql"
EOL

# Make all backup scripts executable
chmod +x script/backup-*.sh

echo "====================== Setup Complete ======================"
echo "MySQL Container Status: $(docker container inspect bor-db --format '{{.State.Status}}' 2>/dev/null)"
echo "Connect to MySQL: docker exec -it bor-db mysql -u ${MYSQL_ALL_SVC_USER} -p${MYSQL_ALL_SVC_USER_PASSWORD}"
echo "Backup scripts created in script/ directory"
echo "Setup completed at $(date)"
