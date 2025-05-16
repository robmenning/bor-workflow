#!/bin/bash

# Ensure python3-venv is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install it first."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create necessary directories if they don't exist
mkdir -p src/workflows tests init-scripts

# Create PostgreSQL initialization script if it doesn't exist
if [ ! -f "init-scripts/01-init.sql" ]; then
    echo "Creating PostgreSQL initialization script..."
    cat > init-scripts/01-init.sql << 'EOF'
-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create schema for Prefect
CREATE SCHEMA IF NOT EXISTS prefect;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE prefect TO prefect;
GRANT ALL PRIVILEGES ON SCHEMA prefect TO prefect;
EOF
fi

# Make scripts executable
chmod +x scripts/*.sh

echo "Development environment setup complete!"
echo "To start the containers, run: ./scripts/manage-containers.sh start"
echo "To activate the virtual environment, run: source venv/bin/activate" 