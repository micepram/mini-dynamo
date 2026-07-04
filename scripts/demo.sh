#!/usr/bin/env bash
# Curl demo against a running `docker compose up` cluster (node1=8081, node2=8082, node3=8083).
# Writes on one node, reads from another to show replication + any-node coordination.
set -euo pipefail

N1=http://localhost:8081
N2=http://localhost:8082
N3=http://localhost:8083

echo "PUT color=blue via node1 (W=2 quorum)"
curl -fsS -X PUT -d 'blue' "$N1/kv/color" >/dev/null && echo "  ok"

echo "GET color via node2 (R=2, different coordinator):"
echo "  $(curl -fsS "$N2/kv/color")"

echo "GET color via node3:"
echo "  $(curl -fsS "$N3/kv/color")"

echo "DELETE color via node2"
curl -fsS -X DELETE "$N2/kv/color" >/dev/null && echo "  ok"

echo "GET color via node1 (expect 404):"
curl -s -o /dev/null -w "  HTTP %{http_code}\n" "$N1/kv/color"
