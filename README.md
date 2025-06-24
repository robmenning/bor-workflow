# BOR Workflow Service

A workflow management service implemented with Prefect orchestration engine for the BOR project.

## Architecture

The service consists of three main components:

1. **bor-workflow**: Prefect server/UI/API container
2. **bor-workflow-db**: PostgreSQL database for Prefect state
3. **bor-etl-agent**: Custom ETL agent for workflow execution

## Development Setup

1. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate  # Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start the services:
```bash
./scripts/manage-containers.sh start
```

## Project Structure

```
bor-workflow/
├── src/
│   ├── workflows/          # Prefect workflow definitions
│   └── utils/             # Utility functions and helpers
├── scripts/               # Management and deployment scripts
├── tests/                 # Test files
├── docker/               # Docker configuration files
└── .env                  # Environment configuration
```

## Initial Workflow

The first workflow implements a simple ETL process:
1. Check for import files in shared volume
2. Load data into staging table
3. Execute stored procedure for data processing

## Environment Variables

Required environment variables are defined in:
- `.env`: Non-sensitive configuration
- `.env.local`: Sensitive configuration (not tracked in git)

## Development

1. Start services:
```bash
./scripts/manage-containers.sh start
```

2. Deploy workflow:
```bash
./scripts/deploy-wf.sh
```

3. Monitor in Prefect UI: http://localhost:4440

## Production Deployment

See deployment section in project documentation for production deployment instructions.
