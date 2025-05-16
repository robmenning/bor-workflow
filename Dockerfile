FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv

# Create prefect user and group
RUN groupadd -g 1000 prefect && \
    useradd -u 1000 -g prefect -m -s /bin/bash prefect

# Set proper permissions for virtual environment
RUN chown -R prefect:prefect /opt/venv

# Switch to prefect user for package installation
USER prefect

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH"
ENV PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@bor-workflow-db:5432/prefect"
# ENV PREFECT_API_URL="http://0.0.0.0:4200/api"
ENV PREFECT_API_URL="http://localhost:4440/api"

# Copy requirements and install dependencies as prefect user
COPY --chown=prefect:prefect requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=prefect:prefect src/ src/
COPY --chown=prefect:prefect tests/ tests/

# Expose port
EXPOSE 4200

# Start Prefect server
CMD ["prefect", "server", "start", "--host", "0.0.0.0", "--port", "4200"] 