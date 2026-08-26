#!/usr/bin/env bash
set -eo pipefail

echo "=== Starting Callsheet Production Server ==="

PORT="${PORT:-8080}"
MCP_PORT="${MCP_PORT:-8000}"

# Start mcp-grafana background server if binary exists
if command -v mcp-grafana >/dev/null 2>&1; then
    echo "Launching mcp-grafana server on 127.0.0.1:${MCP_PORT}..."
    mcp-grafana -t streamable-http -address "127.0.0.1:${MCP_PORT}" -endpoint-path "/mcp" -log-level info &
    MCP_PID=$!
elif [ -f "./bin/mcp-grafana" ]; then
    echo "Launching local ./bin/mcp-grafana server on 127.0.0.1:${MCP_PORT}..."
    ./bin/mcp-grafana -t streamable-http -address "127.0.0.1:${MCP_PORT}" -endpoint-path "/mcp" -log-level info &
    MCP_PID=$!
else
    echo "WARNING: mcp-grafana binary not found. Running web service only."
fi

# Cleanup on exit
trap 'echo "Stopping processes..."; kill $(jobs -p) 2>/dev/null || true; exit' SIGINT SIGTERM

echo "Starting Uvicorn web application on 0.0.0.0:${PORT}..."
exec uvicorn callsheet.web.app:app --host 0.0.0.0 --port "$PORT"
