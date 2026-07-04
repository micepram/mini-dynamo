# mini-dynamo

A from-scratch reimplementation of the core ideas in Amazon's Dynamo
(DeCandia et al., SOSP 2007), scoped as a portfolio and learning project.

**Stack:** Java 21, Spring Boot 3.x, Gradle (single module), Docker Compose.
**Conflict model:** last-write-wins (LWW) using a Lamport logical clock per key
with a node-id tiebreak. No master; every node runs identical code.

> Authoritative spec: [`docs/spec.md`](docs/spec.md). Working conventions:
> [`CLAUDE.md`](CLAUDE.md). Design decisions & tradeoffs: [`DESIGN.md`](DESIGN.md).

## Status

Built in tiers (spec §10). Current: **Tier 3 — membership & resilience** next.

- [x] Tier 0 — single node: `get`/`put`/`delete` over REST, pluggable storage.
- [x] Tier 1 — ring & replication: consistent hashing, preference list, N/R/W quorum.
- [x] Tier 2 — LWW versioning & read repair: Lamport clock, deterministic resolver, tombstone convergence.
- [ ] Tier 3 — membership (gossip), sloppy quorum, hinted handoff.
- [ ] Tier 4 (stretch) — Merkle anti-entropy, tombstone GC, metrics.

## Build & test

```bash
make build      # ./gradlew build — compile + all tests
make test       # tests only
```

JDK 21 is required; the Makefile pins `JAVA_HOME` to a Java 21 home.

## Run

Single node:

```bash
NODE_ID=node1 SEEDS=node1:8080 STORAGE_ENGINE=inmemory ./gradlew bootRun
```

3-node cluster (nodes on host ports 8081–8083):

```bash
make up        # docker compose up --build -d
make demo      # curl: write on node1, read from node2/node3
make down
```

> **Testcontainers note:** `ClusterIntegrationTest` stands the cluster up
> automatically, but auto-skips if Testcontainers can't reach Docker. On recent
> Docker Desktop (engine API min ≥ 1.44) the bundled docker-java may fail
> detection; the Tier 1 gate is then verified via `make up` + `make demo`.

## Configuration

Environment-driven, bound through one `@ConfigurationProperties("minidynamo")`
class. See [`CLAUDE.md`](CLAUDE.md) §5 for the full key/default table.
