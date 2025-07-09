FROM prefecthq/prefect:3-latest

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-mysql-client \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory
WORKDIR /opt/prefect

# Copy application code
COPY src/ /opt/prefect/src/
COPY prefect.yaml /opt/prefect/prefect.yaml

# Copy management scripts
COPY scripts/ /opt/prefect/scripts/

# Set environment variables
ENV PYTHONPATH=/opt/prefect

# Create necessary directories
RUN mkdir -p /data/imports && chmod 777 /data/imports

# Expose the correct port
EXPOSE 8080

# Health check with correct port
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/api/health || exit 1

# Default command with explicit port
CMD ["prefect", "server", "start", "--host", "0.0.0.0", "--port", "8080"]