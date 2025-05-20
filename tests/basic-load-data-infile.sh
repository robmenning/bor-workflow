#!/bin/bash

# Exit on any error
set -e

# Source environment variables
# Parse command line arguments
ENV="development"  # Default to development
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)
            ENV="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Source environment variables
if [ -f ../.env ]; then
    source ../.env
fi
if [ -f ../.env.${ENV}.local ]; then
    source ../.env.${ENV}.local
fi

# Configuration
FTP_USER="ftpetl"
FTP_PASS="lteptf"  # From envs.txt
FTP_HOST="localhost"
FTP_PORT="4470"    # Dev port from port scheme
CSV_FILE="tests/data/fund-class-fees.csv"
DB_HOST="bor-db"
DB_USER="borAllSvc"  # Using borAllSvc for multi-database access
DB_PASS="u67nyNgomZ"  # From envs.txt
DB_NAME="borarch"
DB_TABLE="FundClassFee"

echo "Starting basic LOAD DATA INFILE test..."

# 1. FTP the file
echo "FTPing file to bor-files container..."
ftp -n -v << EOF
open $FTP_HOST $FTP_PORT
user $FTP_USER $FTP_PASS
binary
cd incoming
put $CSV_FILE fund-class-fees.csv
ls -l
bye
EOF

FTP_STATUS=$?
if [ $FTP_STATUS -ne 0 ]; then
    echo "Error: FTP upload failed with status $FTP_STATUS"
    exit 1
fi
echo "FTP upload successful"

# 2. Copy file directly to bor-etl-files volume
echo "Copying file to bor-etl-files volume..."
docker cp $CSV_FILE bor-db:/var/lib/mysql-files/fund-class-fees.csv

# 3. Import the file using LOAD DATA INFILE
echo "Importing file into database..."
docker exec -i bor-db mysql \
    --user=$DB_USER \
    --password=$DB_PASS \
    --host=$DB_HOST \
    $DB_NAME \
    -e "LOAD DATA INFILE '/var/lib/mysql-files/fund-class-fees.csv'
        INTO TABLE $DB_TABLE
        FIELDS TERMINATED BY ','
        ENCLOSED BY '\"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (FundCode, FundName, Class, Description, Mer, @Trailer, @PerformanceFee, 
         @MinInvestmentInitial, @MinInvestmentSubsequent, Currency)
        SET 
            Trailer = NULLIF(@Trailer, ''),
            PerformanceFee = NULLIF(@PerformanceFee, ''),
            MinInvestmentInitial = NULLIF(@MinInvestmentInitial, ''),
            MinInvestmentSubsequent = NULLIF(@MinInvestmentSubsequent, '');"

if [ $? -ne 0 ]; then
    echo "Error: LOAD DATA INFILE failed"
    exit 1
fi
echo "Import successful"

# 4. Clean up the temporary file
echo "Cleaning up temporary file..."
docker exec -i bor-db rm /var/lib/mysql-files/fund-class-fees.csv

# 5. Execute the stored procedure in bormeta database
echo "Executing stored procedure..."
docker exec -i bor-db mysql \
    --user=$DB_USER \
    --password=$DB_PASS \
    --host=$DB_HOST \
    bormeta \
    -e "CALL usp_FundClassFee_Load();"

if [ $? -ne 0 ]; then
    echo "Error: Stored procedure execution failed"
    exit 1
fi
echo "Stored procedure execution successful"

# 6. Verify the import
echo "Verifying import..."
docker exec -i bor-db mysql \
    --user=$DB_USER \
    --password=$DB_PASS \
    --host=$DB_HOST \
    $DB_NAME \
    -e "SELECT COUNT(*) as row_count FROM $DB_TABLE;"

echo "Test completed successfully"
