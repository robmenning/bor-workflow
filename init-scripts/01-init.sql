-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create schema for Prefect
CREATE SCHEMA IF NOT EXISTS prefect;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE prefect TO prefect;
GRANT ALL PRIVILEGES ON SCHEMA prefect TO prefect; 