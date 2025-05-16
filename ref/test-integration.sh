#!/bin/bash

# Load environment variables if they exist
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi
if [ -f .env.local ]; then
  export $(grep -v '^#' .env.local | xargs)
fi

# Default values
FTP_HOME="/home/vsftpd"
FTP_USER=${FTP_ETL_USER:-"ftpetl"}
FTP_PASS=${FTP_ETL_PASS:-"missing"}

# Validate credentials
if [ "$FTP_PASS" = "missing" ]; then
  echo "Error: FTP password not found in environment variables."
  echo "Please ensure .env or .env.local contains FTP_ETL_PASS."
  exit 1
fi

# Required tools
REQUIRED_TOOLS=("docker" "curl" "nc" "ftp" "timeout")
for tool in "${REQUIRED_TOOLS[@]}"; do
  if ! command -v $tool &> /dev/null; then
    echo "Error: Required tool '$tool' is not installed."
    exit 1
  fi
done

# Function to check container health
check_container_health() {
  local container=$1
  if ! docker ps | grep -q $container; then
    echo "Warning: $container container is not running."
    return 1
  fi
  return 0
}

# Function to run a test and report result
run_test() {
  local test_name=$1
  local test_command=$2
  local timeout=${3:-30}  # Default timeout of 30 seconds
  
  echo "Running test: $test_name"
  if timeout $timeout bash -c "$test_command" 2>/dev/null; then
    echo "✅ Test passed: $test_name"
    return 0
  else
    local exit_code=$?
    if [ $exit_code -eq 124 ]; then
      echo "❌ Test failed: $test_name (timeout after ${timeout}s)"
    else
      echo "❌ Test failed: $test_name (exit code: $exit_code)"
    fi
    return 1
  fi
}

# Function to run docker command with timeout
run_docker_cmd() {
  local cmd=$1
  local timeout=${2:-10}
  timeout $timeout docker exec bor-files sh -c "$cmd" 2>/dev/null
  return $?
}

# Determine environment and port
ENV="development"
if [ "$1" == "prod" ] || [ "$1" == "production" ]; then
  ENV="production"
  HOST_PORT=4670
elif [ "$1" == "stage" ] || [ "$1" == "staging" ]; then
  ENV="staging"
  HOST_PORT=4570
else
  HOST_PORT=4470
fi

echo "Running integration tests in $ENV environment (port $HOST_PORT)..."
echo "Using FTP credentials: $FTP_USER / $FTP_PASS"

# Check if required containers are running
check_container_health "bor-files" || exit 1

# Create test files
echo "Creating test files..."
TEST_FILE="/tmp/bor-files-test-$(date +%s).txt"
echo "BOR Files Integration Test" > "$TEST_FILE"
echo "Date: $(date)" >> "$TEST_FILE"
echo "Environment: $ENV" >> "$TEST_FILE"

# Test 1: Basic FTP Server Connectivity
run_test "FTP Server Connectivity" "nc -zv localhost $HOST_PORT 21" 5

# Test 2: FTP Login
run_test "FTP Login" "curl -v --connect-timeout 5 -u $FTP_USER:$FTP_PASS ftp://localhost:$HOST_PORT/" 10

# Test 3: File Upload
run_test "File Upload" "curl -v --connect-timeout 5 -T $TEST_FILE ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/test_upload.txt" 10

# Test 4: File Download
run_test "File Download" "curl -v --connect-timeout 5 -o /tmp/downloaded.txt ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/test_upload.txt" 10

# Test 5: Directory Creation
run_test "Directory Creation" "curl -v --connect-timeout 5 -X MKD ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/test_dir/" 10

# Test 6: Directory Listing
run_test "Directory Listing" "curl -v --connect-timeout 5 -l ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/" 10

# Test 7: File Permissions
run_test "File Permissions" "run_docker_cmd 'ls -l $FTP_HOME/$FTP_USER/incoming/test_upload.txt' 5 | grep -q '^-rw-r--r--' || run_docker_cmd 'ls -l $FTP_HOME/$FTP_USER/incoming/test_upload.txt' 5 | grep -q '^-rw-rw-r--'" 10

# Test 8: Passive Mode
run_test "Passive Mode" "curl -v --connect-timeout 5 -P - ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/test_upload.txt" 10

# Test 9: Large File Transfer (10MB)
echo "Testing large file transfer..."
dd if=/dev/zero of=/tmp/large_test_file bs=1M count=10
run_test "Large File Transfer" "curl -v --connect-timeout 30 -T /tmp/large_test_file ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/large_test_file.txt" 60

# Test 10: Concurrent Transfers
echo "Testing concurrent transfers..."
for i in {1..3}; do
  cp "$TEST_FILE" "/tmp/test_file_$i.txt"
  curl -s --connect-timeout 5 -T "/tmp/test_file_$i.txt" "ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/concurrent_$i.txt" &
done
wait
run_test "Concurrent Transfers" "run_docker_cmd 'ls -l $FTP_HOME/$FTP_USER/incoming/ | grep -c concurrent_' 5 | grep -q '3'" 10

# Test 11: Container Network Access (Optional)
if check_container_health "bor-workflow"; then
  run_test "Workflow Container Access" "docker exec bor-workflow curl -s --connect-timeout 5 ftp://$FTP_USER:$FTP_PASS@bor-files:21/incoming/test_upload.txt" 10
else
  echo "Skipping Workflow Container Access test (container not running)"
fi

if check_container_health "bor-etl"; then
  run_test "ETL Container Access" "docker exec bor-etl curl -s --connect-timeout 5 ftp://$FTP_USER:$FTP_PASS@bor-files:21/incoming/test_upload.txt" 10
else
  echo "Skipping ETL Container Access test (container not running)"
fi

# Test 12: File Processing Workflow
echo "Testing file processing workflow..."
run_test "Move to Processed" "run_docker_cmd 'mv $FTP_HOME/$FTP_USER/incoming/test_upload.txt $FTP_HOME/$FTP_USER/processed/' 10" 15
run_test "Processed File Exists" "run_docker_cmd 'test -f $FTP_HOME/$FTP_USER/processed/test_upload.txt' 5" 10

# Test 13: Archive Functionality
run_test "Move to Archive" "run_docker_cmd 'mv $FTP_HOME/$FTP_USER/processed/test_upload.txt $FTP_HOME/$FTP_USER/archive/' 10" 15
run_test "Archived File Exists" "run_docker_cmd 'test -f $FTP_HOME/$FTP_USER/archive/test_upload.txt' 5" 10

# Test 14: Quota Enforcement (if enabled)
if run_docker_cmd "quota -s $FTP_USER" 5 &>/dev/null; then
  echo "Testing quota enforcement..."
  # Create a file larger than quota
  dd if=/dev/zero of=/tmp/quota_test bs=1M count=1000
  run_test "Quota Enforcement" "! curl -v --connect-timeout 5 -T /tmp/quota_test ftp://$FTP_USER:$FTP_PASS@localhost:$HOST_PORT/incoming/quota_test.txt" 30
  rm -f /tmp/quota_test
fi

# Clean up
echo "Cleaning up test files..."
rm -f "$TEST_FILE" /tmp/downloaded.txt /tmp/large_test_file /tmp/test_file_*.txt
run_docker_cmd "rm -rf $FTP_HOME/$FTP_USER/incoming/test_* $FTP_HOME/$FTP_USER/processed/test_* $FTP_HOME/$FTP_USER/archive/test_* $FTP_HOME/$FTP_USER/incoming/test_dir" 10

echo "Integration tests completed." 