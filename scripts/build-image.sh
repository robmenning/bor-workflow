#!/bin/bash

# Build script for Azure/Kubernetes deployment
# This script builds the bor-workflow image that can be pushed to Azure Container Registry

set -e

# Configuration
IMAGE_NAME="bor-workflow"
TAG=${1:-latest}
REGISTRY=${2:-""}  # Optional registry prefix (e.g., "myregistry.azurecr.io/")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building bor-workflow image for Azure/Kubernetes deployment...${NC}"
echo "Image: ${REGISTRY}${IMAGE_NAME}:${TAG}"

# Check if Dockerfile exists
if [ ! -f "Dockerfile" ]; then
    echo -e "${RED}Error: Dockerfile not found in current directory${NC}"
    exit 1
fi

# Build the image
echo -e "${YELLOW}Building Docker image...${NC}"
docker build -t "${REGISTRY}${IMAGE_NAME}:${TAG}" .

# Verify the image was created
if docker images | grep -q "${IMAGE_NAME}"; then
    echo -e "${GREEN}✓ Image built successfully: ${REGISTRY}${IMAGE_NAME}:${TAG}${NC}"
    
    # Show image details
    echo -e "${YELLOW}Image details:${NC}"
    docker images "${REGISTRY}${IMAGE_NAME}:${TAG}"
    
    # Show image layers
    echo -e "${YELLOW}Image layers:${NC}"
    docker history "${REGISTRY}${IMAGE_NAME}:${TAG}" --no-trunc | head -10
    
else
    echo -e "${RED}✗ Failed to build image${NC}"
    exit 1
fi

echo -e "${GREEN}Build completed successfully!${NC}"
echo ""
echo "To push to Azure Container Registry:"
echo "  docker push ${REGISTRY}${IMAGE_NAME}:${TAG}"
echo ""
echo "To run locally for testing:"
echo "  docker run -d --name bor-workflow-test -p 4440:4200 ${REGISTRY}${IMAGE_NAME}:${TAG}" 