# Azure/Kubernetes Deployment Guide

This guide explains how to deploy the bor-workflow service to Azure Container Registry and Kubernetes.

## Prerequisites

- Azure CLI installed and configured
- Docker installed and running
- Access to Azure Container Registry (ACR)
- Kubernetes cluster (AKS) configured

## Step 1: Build the Docker Image

Build the image using the provided script:

```bash
# Build with default tag (latest)
./scripts/build-image.sh

# Build with specific tag
./scripts/build-image.sh v1.0.0

# Build with Azure Container Registry prefix
./scripts/build-image.sh v1.0.0 myregistry.azurecr.io/
```

## Step 2: Tag and Push to Azure Container Registry

```bash
# Login to Azure Container Registry
az acr login --name myregistry

# Tag the image for ACR
docker tag bor-workflow:latest myregistry.azurecr.io/bor-workflow:v1.0.0

# Push to ACR
docker push myregistry.azurecr.io/bor-workflow:v1.0.0
```

## Step 3: Deploy to Kubernetes

The infrastructure repository should contain Kubernetes manifests. The image reference should be:

```yaml
image: myregistry.azurecr.io/bor-workflow:v1.0.0
```

## Architecture Differences

### Current Docker Setup (3 containers)
- `bor-workflow`: Prefect server (official image)
- `bor-workflow-db`: PostgreSQL database
- `bor-etl-agent`: ETL agent (custom image)

### Azure/Kubernetes Setup (1 image)
- Single `bor-workflow` image containing all Prefect components
- PostgreSQL database managed separately (Azure Database for PostgreSQL or container)
- ETL agent runs as a separate pod/deployment

## Environment Variables

Ensure these environment variables are configured in your Kubernetes deployment:

```yaml
env:
- name: PREFECT_API_DATABASE_CONNECTION_URL
  value: "postgresql+asyncpg://user:password@postgres-host:5432/prefect"
- name: PREFECT_API_URL
  value: "http://bor-workflow-service:4200/api"
- name: DB_HOST
  value: "your-mysql-host"
- name: DB_PORT
  value: "3306"
- name: DB_USER
  valueFrom:
    secretKeyRef:
      name: bor-secrets
      key: db-user
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: bor-secrets
      key: db-password
- name: DB_NAME
  value: "borarch"
```

## Testing

Before deploying to production:

1. Test locally with custom image:
```bash
./scripts/manage-containers.sh start dev --use-custom-image
```

2. Run compatibility tests:
```bash
./scripts/test-backward-compatibility.sh
```

3. Verify Prefect UI is accessible at `http://localhost:4440`

## Rollback Strategy

If issues arise with the new deployment:

1. Revert to the previous image tag in Kubernetes
2. Continue using the existing Docker setup with `manage-containers.sh`
3. The backward compatibility ensures no disruption to existing workflows

## Monitoring

Monitor the deployment using:

- Kubernetes dashboard
- Azure Monitor
- Prefect UI (accessible via ingress/service)
- Container logs: `kubectl logs -f deployment/bor-workflow` 