"""
Queries Grafana Cloud Prometheus for render_farm_node_temperature_celsius
over a specified duration to prove continuous unthrottled background emission without gaps.
"""

import os
import sys
import time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_TOKEN = os.getenv("GRAFANA_SERVICE_ACCOUNT_TOKEN")

if not GRAFANA_URL or not GRAFANA_TOKEN:
    print("Error: Missing GRAFANA_URL or GRAFANA_SERVICE_ACCOUNT_TOKEN in .env")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {GRAFANA_TOKEN}",
    "Content-Type": "application/json",
}

# Range query over Prometheus
now = int(time.time())
duration_seconds = int(sys.argv[1]) if len(sys.argv) > 1 else 3600
start = now - duration_seconds
step = 5  # 5-second step

query_url = f"{GRAFANA_URL}/api/datasources/proxy/uid/grafanacloud-prom/api/v1/query_range"
params = {
    "query": 'render_farm_node_temperature_celsius{node_id="node-01"}',
    "start": start,
    "end": now,
    "step": step,
}

print(f"Querying Prometheus range [{datetime.fromtimestamp(start, tz=timezone.utc).isoformat()} to {datetime.fromtimestamp(now, tz=timezone.utc).isoformat()}]...")
resp = requests.get(query_url, headers=headers, params=params)

if resp.status_code != 200:
    print(f"Error querying Prometheus: {resp.status_code} {resp.text}")
    sys.exit(1)

data = resp.json()
result = data.get("data", {}).get("result", [])

if not result:
    print("No series returned.")
    sys.exit(1)

values = result[0].get("values", [])
print(f"Total samples retrieved: {len(values)}")

if not values:
    print("No values found in window.")
    sys.exit(0)

first_ts = values[0][0]
last_ts = values[-1][0]
span_minutes = (last_ts - first_ts) / 60.0
print(f"First sample: {datetime.fromtimestamp(first_ts, tz=timezone.utc).isoformat()}")
print(f"Last sample:  {datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()}")
print(f"Span: {span_minutes:.1f} minutes")

# Check for gaps > 30 seconds
gaps = []
for i in range(1, len(values)):
    prev_t = values[i-1][0]
    curr_t = values[i][0]
    diff = curr_t - prev_t
    if diff > 30:
        gaps.append((prev_t, curr_t, diff))

if gaps:
    print(f"Found {len(gaps)} gaps > 30s:")
    for prev_t, curr_t, diff in gaps:
        print(f"  Gap of {diff}s from {datetime.fromtimestamp(prev_t, tz=timezone.utc).isoformat()} to {datetime.fromtimestamp(curr_t, tz=timezone.utc).isoformat()}")
else:
    print(f"SUCCESS: Continuous telemetry confirmed. Zero gaps > 30s across {len(values)} consecutive samples.")
