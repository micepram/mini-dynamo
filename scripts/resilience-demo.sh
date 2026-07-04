#!/usr/bin/env bash
# Tier 3 resilience demo against a running `docker compose up` cluster.
# Shows: sloppy quorum keeps writes available with a node down, and hinted handoff
# delivers the missed write when the node returns (nodes converge).
set -euo pipefail

N1=http://localhost:8081
N3_INTERNAL=http://localhost:8083/internal/kv

echo "1) kill node3"
docker compose stop node3 >/dev/null
echo "   waiting ~13s for failure detection (ALIVE -> SUSPECT -> DEAD)..."
sleep 13

echo "2) write key 'resil' via node1 while node3 is DEAD (sloppy quorum, W=2)"
curl -fsS -o /dev/null -w "   PUT -> HTTP %{http_code}\n" -X PUT -d 'survived-the-outage' "$N1/kv/resil"

echo "3) restart node3, waiting ~10s for revival + hinted handoff..."
docker compose start node3 >/dev/null
sleep 10

echo "4) node3's own store after recovery (expect 200 — hint delivered, not read-repair):"
curl -s -o /dev/null -w "   internal GET node3 -> HTTP %{http_code}\n" "$N3_INTERNAL/resil"

echo "5) convergence — read 'resil' from every node:"
for p in 8081 8082 8083; do printf "   :%s -> " "$p"; curl -fsS "http://localhost:$p/kv/resil"; echo; done
