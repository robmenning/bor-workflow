-- Create prefect user and database
CREATE USER prefect WITH PASSWORD 'prefect' SUPERUSER;
CREATE DATABASE prefect OWNER prefect;

-- Connect to prefect database
\c prefect

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create schema and grant permissions
CREATE SCHEMA IF NOT EXISTS prefect;
GRANT ALL ON SCHEMA prefect TO prefect;
GRANT ALL ON ALL TABLES IN SCHEMA prefect TO prefect;
GRANT ALL ON ALL SEQUENCES IN SCHEMA prefect TO prefect; 