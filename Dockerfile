# Production Dockerfile for Callsheet
FROM python:3.12-slim

# Install system utilities and curl for binary provisioning
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Download official mcp-grafana Linux x86_64 binary
RUN curl -fsSL "https://github.com/grafana/mcp-grafana/releases/download/v1.2.0/mcp-grafana_Linux_x86_64.tar.gz" | tar -xz -C /usr/local/bin/ \
    && chmod +x /usr/local/bin/mcp-grafana

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .
RUN pip install --no-cache-dir --no-deps -e .

EXPOSE 8080

CMD ["./scripts/start_server.sh"]
