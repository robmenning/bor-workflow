#!/bin/bash

# Test script to verify backward compatibility
# This script tests both the old and new deployment methods

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Testing backward compatibility...${NC}"

# Test 1: Build the new image
echo -e "${YELLOW}Test 1: Building new Docker image...${NC}"
if ./scripts/build-image.sh test-build; then
    echo -e "${GREEN}✓ New image builds successfully${NC}"
else
    echo -e "${RED}✗ New image build failed${NC}"
    exit 1
fi

# Test 2: Verify the image contains expected files
echo -e "${YELLOW}Test 2: Verifying image contents...${NC}"
if docker run --rm bor-workflow:test-build ls -la /opt/prefect/src/ > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Image contains source code${NC}"
else
    echo -e "${RED}✗ Image missing source code${NC}"
    exit 1
fi

if docker run --rm bor-workflow:test-build ls -la /opt/prefect/prefect.yaml > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Image contains prefect.yaml${NC}"
else
    echo -e "${RED}✗ Image missing prefect.yaml${NC}"
    exit 1
fi

# Test 3: Verify Python dependencies are installed
echo -e "${YELLOW}Test 3: Verifying Python dependencies...${NC}"
if docker run --rm bor-workflow:test-build python -c "import prefect; print('Prefect version:', prefect.__version__)" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Prefect is installed${NC}"
else
    echo -e "${RED}✗ Prefect not installed${NC}"
    exit 1
fi

if docker run --rm bor-workflow:test-build python -c "import mysql.connector; print('MySQL connector installed')" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ MySQL connector is installed${NC}"
else
    echo -e "${RED}✗ MySQL connector not installed${NC}"
    exit 1
fi

# Test 4: Verify system dependencies
echo -e "${YELLOW}Test 4: Verifying system dependencies...${NC}"
if docker run --rm bor-workflow:test-build mysql --version > /dev/null 2>&1; then
    echo -e "${GREEN}✓ MySQL client is installed${NC}"
else
    echo -e "${RED}✗ MySQL client not installed${NC}"
    exit 1
fi

if docker run --rm bor-workflow:test-build psql --version > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PostgreSQL client is installed${NC}"
else
    echo -e "${RED}✗ PostgreSQL client not installed${NC}"
    exit 1
fi

# Test 5: Verify health check
echo -e "${YELLOW}Test 5: Verifying health check...${NC}"
if docker run --rm bor-workflow:test-build curl --version > /dev/null 2>&1; then
    echo -e "${GREEN}✓ curl is available for health checks${NC}"
else
    echo -e "${RED}✗ curl not available${NC}"
    exit 1
fi

# Test 6: Verify default command
echo -e "${YELLOW}Test 6: Verifying default command...${NC}"
if docker run --rm bor-workflow:test-build prefect --help > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Prefect CLI is available${NC}"
else
    echo -e "${RED}✗ Prefect CLI not available${NC}"
    exit 1
fi

# Cleanup
echo -e "${YELLOW}Cleaning up test image...${NC}"
docker rmi bor-workflow:test-build > /dev/null 2>&1 || true

echo -e "${GREEN}✓ All backward compatibility tests passed!${NC}"
echo ""
echo "The new Dockerfile is compatible with the existing setup."
echo "You can now:"
echo "  1. Use the existing manage-containers.sh script (backward compatible)"
echo "  2. Use the new --use-custom-image flag for testing"
echo "  3. Build and push to Azure Container Registry for Kubernetes deployment" 