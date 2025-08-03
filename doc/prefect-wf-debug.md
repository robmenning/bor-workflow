# Prefect Workflow Debugging Guide

# to remove the deployments:
<!-- 
docker exec bor-workflow prefect deployment delete "import-web-classfees-flow/Import web classfees"
docker exec bor-workflow prefect deployment delete "import-web-hold-flow/Import web hold" 
-->

## Git History Cleanup (for secrets)

### Method 1: Soft Reset (Quick)
```bash
# Remove secrets from current commit
git reset --soft HEAD~1
# Edit files to remove secrets
git add .
git commit -m "clean commit message"
git push --force origin <branch>
```

### Method 2: Complete History Cleanup (Thorough)
```bash
# Create new clean branch
git checkout --orphan clean-branch
git add .
git commit -m "clean initial commit"

# Force reset original branch
git checkout <original-branch>
git reset --hard clean-branch
git push --force origin <original-branch>

# Clean up
git branch -D clean-branch
```

## Prefect Environment Setup

### Activate Virtual Environment
```bash
source venv/bin/activate
```

### Check Prefect Version
```bash
prefect version
```

## Container Management

### Start/Stop Containers
```bash
# Development
./scripts/manage-containers.sh start dev

# Production  
./scripts/manage-containers.sh start prod

# Stop all
./scripts/manage-containers.sh stop

# Clear containers
./scripts/manage-containers.sh clear
```

### Check Container Status
```bash
docker ps
docker logs bor-workflow
docker logs bor-etl-agent
```

## Workflow Deployment

### Deploy All Workflows
```bash
./scripts/deploy-wf.sh
# or manually:
docker exec -it bor-workflow prefect deploy --all
```

### Deploy Specific Workflow
```bash
docker exec -it bor-workflow prefect deployment build src/workflows/mellon_holdings_etl.py:mellon_holdings_etl_flow -n "Mellon Holdings ETL" -q "default"
docker exec -it bor-workflow prefect deployment apply mellon_holdings_etl_flow-deployment.yaml
```

## Workflow Execution

### Run Workflow from Container
```bash
docker exec -it bor-workflow prefect deployment run "mellon-holdings-etl/Mellon Holdings ETL" --param file_names='["mellon-660600017-AAD-20250414.csv"]'
```

### Run with Multiple Files
```bash
docker exec -it bor-workflow prefect deployment run "mellon-holdings-etl/Mellon Holdings ETL" --param file_names='["mellon-660600017-AAD-20250414.csv", "mellon-660600027-AAD-20250414.csv", "mellon-660610007-AAD-20250414.csv"]'
```

## Database Debugging

### Check Database Connection
```bash
docker exec -it bor-db mysql -u borETLSvc -pu67nomZyNg -e "SELECT 1;"
```

### Check Table Structure
```bash
docker exec -it bor-db mysql -u borETLSvc -pu67nomZyNg -e "USE borarch; DESCRIBE MellonHoldingsStaging;"
```

### Check Data in Tables
```bash
docker exec -it bor-db mysql -u borETLSvc -pu67nomZyNg -e "USE borarch; SELECT COUNT(*) FROM MellonHoldingsStaging;"
docker exec -it bor-db mysql -u borETLSvc -pu67nomZyNg -e "USE borarch; SELECT * FROM MellonHoldingsStaging LIMIT 5;"
```

### Check File Import Tracking
```bash
docker exec -it bor-db mysql -u borETLSvc -pu67nomZyNg -e "USE borarch; SELECT * FROM MellonFileImport;"
```

## File Management

### Copy Test Files to Container
```bash
# cd to workflow
docker cp tests/data/mellon-660600017-AAD-20250414.csv bor-db:/var/lib/mysql-files/ftpetl/incoming/
docker cp tests/data/mellon-660600027-AAD-20250414.csv bor-db:/var/lib/mysql-files/ftpetl/incoming/
docker cp tests/data/mellon-660610007-AAD-20250414.csv bor-db:/var/lib/mysql-files/ftpetl/incoming/
```

### Verify Files in Container
```bash
docker exec bor-db ls -la /var/lib/mysql-files/ftpetl/incoming/mellon-*.csv
docker exec bor-db head -5 /var/lib/mysql-files/ftpetl/incoming/mellon-660600017-AAD-20250414.csv
```

## ETL Agent Management

### Rebuild ETL Agent (when code changes)
```bash
# Stop current agent
docker stop bor-etl-agent
docker rm bor-etl-agent

# Rebuild image
docker build -f docker/Dockerfile.etl-agent -t bor-etl-agent:latest .

# Start new agent
docker run -d --rm \
  --name bor-etl-agent \
  --network bor-network \
  -e PREFECT_API_URL="http://bor-workflow:4200/api" \
  bor-etl-agent:latest \
  prefect worker start --work-pool 'default-agent-pool'
```

### Check Agent Logs
```bash
docker logs bor-etl-agent -f
```

## Code Debugging

### Clear Python Cache
```bash
docker exec -it bor-etl-agent find /app -name "*.pyc" -delete
docker exec -it bor-etl-agent find /app -name "__pycache__" -type d -exec rm -rf {} +
```

### Check CSV Column Count
```bash
docker exec bor-db head -1 /var/lib/mysql-files/ftpetl/incoming/mellon-660600017-AAD-20250414.csv | tr ',' '\n' | wc -l
```

### Verify CSV Headers
```bash
docker exec bor-db head -1 /var/lib/mysql-files/ftpetl/incoming/mellon-660600017-AAD-20250414.csv
```

## Common Error Patterns

### MySQL LOAD DATA INFILE Errors
- **Column count mismatch**: Check `field_mappings` count vs CSV columns
- **Data type errors**: Add transformations for empty values, commas, dashes
- **Date format errors**: Use correct `STR_TO_DATE` format strings
- **File path errors**: Ensure file exists in MySQL's `secure_file_priv` directory

### Prefect Deployment Errors
- **Module not found**: Check imports in `src/workflows/__init__.py`
- **Parameter validation**: Ensure parameters match flow definition types
- **Agent not running**: Start worker with `prefect worker start`

### Docker Network Issues
- **Host not found**: Ensure containers are on `bor-network`
- **Connection refused**: Check if target service is running

## Data Transformation Patterns

### Handle Empty Values
```sql
CASE WHEN TRIM(@column) = '' OR TRIM(@column) = ' ' OR TRIM(@column) = '-' THEN NULL ELSE @column END
```

### Remove Commas from Numbers
```sql
NULLIF(REPLACE(REPLACE(TRIM(@column), '\"', ''), ',', ''), '')
```

### Parse Dates
```sql
STR_TO_DATE(TRIM(@column), '%m/%d/%Y')
```

### Handle Column Names with Spaces
```sql
TRIM(@`Column Name`)  -- Use backticks around @ variables
```

## Quick Debug Cycle
1. Make code changes
2. Rebuild ETL agent: `docker build -f docker/Dockerfile.etl-agent -t bor-etl-agent:latest .`
3. Restart agent: Stop old, start new
4. Deploy workflow: `docker exec -it bor-workflow prefect deploy --all`
5. Run workflow: `docker exec -it bor-workflow prefect deployment run ...`
6. Check logs: `docker logs bor-etl-agent -f`
7. Check database: Verify data loaded correctly
