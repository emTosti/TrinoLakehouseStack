# Data Lake Stack — Trino Edition

A local, Docker-based **federated query** environment. [Trino](https://trino.io/)
sits in front of three storage systems and lets you query — and join — across all
of them with plain SQL:

| Catalog | Backed by | Holds |
|---|---|---|
| `iceberg` | [Lakekeeper](https://github.com/lakekeeper/lakekeeper) (Iceberg REST catalog) + MinIO | Managed Apache Iceberg tables |
| `s3` | Hive Metastore + MinIO | External tables over raw Parquet/ORC files |
| `postgresql` | PostgreSQL | Whatever lives in the `postgres` database |

Everything runs in containers on one Docker network; nothing is installed on the
host.

---

## Architecture at a glance

```mermaid
flowchart LR
  subgraph Query
    TC[trino-coordinator] --- TW[trino-worker]
  end
  subgraph Metadata
    LK[lakekeeper<br/>Iceberg REST]
    HMS[hive-metastore]
  end
  PG[(postgres)]
  MINIO[(MinIO<br/>S3 object store)]

  TC -->|iceberg catalog| LK
  TC -->|s3 catalog| HMS
  TC -->|postgresql catalog| PG
  TC -->|reads/writes data files| MINIO
  TW -->|reads/writes data files| MINIO
  LK --> PG
  LK --> MINIO
  HMS --> PG
  HMS --> MINIO
```

- **MinIO** stores the bytes (Parquet/ORC/Iceberg data + metadata files).
- **Lakekeeper** and the **Hive Metastore** only store *table metadata* — where
  the files are, their schema, their layout. Both persist that in PostgreSQL.
- **Trino** asks a catalog "where is this table?", then reads/writes the data
  files on MinIO directly using its native S3 client.
- **PostgreSQL** is doing triple duty: metadata store for Lakekeeper and the Hive
  Metastore, *and* a queryable source in its own right.

More detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Prerequisites

- Docker and the Docker Compose plugin (`docker compose`, v2).
- **Apple Silicon / ARM64.** The Lakekeeper image is pinned to `v0-arm64`. On an
  x86_64 host, change that tag in `docker-compose.yml` — see
  [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md#lakekeeper-image-is-arm-only).
- ~4 GB of free RAM for the containers.

---

## Quick start

```bash
# 1. Create your env file (it is git-ignored)
cp .env.example .env

# 2. Bring the stack up (first run builds the hive-metastore image
#    and pulls images — a few minutes)
docker compose up -d

# 3. Watch it settle; one-shot jobs (minio-setup, lakekeeper_migrate,
#    lakekeeper_prepare) should end with "Exited (0)"
docker compose ps -a

# 4. Open a SQL shell
docker compose exec trino-coordinator trino
```

```sql
trino> SHOW CATALOGS;
 Catalog
------------
 iceberg
 postgresql
 s3
 system
```

To reset everything to a clean slate:

```bash
docker compose down
rm -rf volume/
```

---

## Services and endpoints

| Service | URL / address | Notes |
|---|---|---|
| Trino UI | http://localhost:8080 | Query history, running queries, workers |
| MinIO console | http://localhost:9001 | Browse buckets |
| MinIO S3 API | http://localhost:9000 | |
| Lakekeeper | http://localhost:8181 | Iceberg REST + management API |
| PostgreSQL | `localhost:5432` | |
| Hive Metastore | `thrift://localhost:9083` | Thrift only, no web UI |

Long-running services: `minio`, `postgres`, `lakekeeper`, `hive-metastore`,
`trino-coordinator`, `trino-worker`. One-shot jobs that exit 0 when done:
`minio-setup` (`prepare_buckets`), `lakekeeper_migrate` (`migrate`),
`lakekeeper_prepare`.

---

## Credentials

All credentials live in `.env` (copied from `.env.example`). Defaults:

| Where | User | Password |
|---|---|---|
| MinIO root / console | `minioadmin` | `minioadmin` |
| MinIO — Trino access key | `trino` | `TRpassw0rd!` |
| MinIO — Lakekeeper access key | `lakekeeper` | `LKpassw0rd!` |
| PostgreSQL superuser | `admin` | `admin` |
| PostgreSQL — Lakekeeper role/db | `lakekeeper` | `lakekeeper123!` |
| PostgreSQL — Hive Metastore role/db | `hive` | `hivepassw0rd!` |

These are development defaults and are intentionally weak. See
[Security notes](#security-notes).

---

## Connecting to the services

All ports are published on `localhost`.

### Trino

The engine you actually run queries against. It accepts any username, needs no
password, and does not require you to pick a catalog or schema up front.

**Trino CLI, inside the container** (nothing to install):

```bash
# interactive shell
docker compose exec trino-coordinator trino

# a single statement
docker compose exec trino-coordinator trino --execute "SHOW CATALOGS"
```

At the `trino>` prompt, end statements with `;`, and `quit` to leave.
`--output-format` takes `ALIGNED` (default for the shell), `CSV`, `JSON`, `TSV`,
`MARKDOWN`, …

**Trino CLI, on your host:** download the `trino` jar from the
[client docs](https://trino.io/docs/current/client/cli.html), then:

```bash
trino --server http://localhost:8080 --user analyst
```

**JDBC** (DBeaver, DataGrip, JetBrains, application code):

```
URL:      jdbc:trino://localhost:8080
User:     analyst        (any value)
Password: (leave blank)
SSL:      off
```

Driver: `io.trino:trino-jdbc`. In DBeaver, pick the built-in "Trino" driver and
enter host `localhost`, port `8080`.

**Python:**

```python
from trino.dbapi import connect  # pip install trino
conn = connect(host="localhost", port=8080, user="analyst")
cur = conn.cursor()
cur.execute("SELECT * FROM iceberg.datalake.demo")
print(cur.fetchall())
```

**Web UI:** http://localhost:8080 — monitors running and past queries and worker
health. It has no query editor; use one of the clients above to run SQL.

### PostgreSQL

**`psql`, inside the container:**

```bash
docker compose exec postgres psql -U admin -d postgres
```

**From your host** (any Postgres client):

```
Host:     localhost
Port:     5432
Database: postgres        (also: lakekeeper, metastore)
User:     admin
Password: admin
```

```bash
psql "postgresql://admin:admin@localhost:5432/postgres"
```

The `lakekeeper` and `metastore` databases belong to Lakekeeper and the Hive
Metastore respectively — you can inspect them, but don't write to them by hand.

### MinIO (S3 object storage)

**Web console:** http://localhost:9001, login `minioadmin` / `minioadmin`.

**`mc` (MinIO client), inside the setup container:**

```bash
docker compose run --rm --entrypoint sh prepare_buckets -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls -r local/warehouse"
```

**`mc` on your host:**

```bash
mc alias set datalake http://localhost:9000 minioadmin minioadmin
mc ls -r datalake/warehouse
```

**AWS CLI / any S3 SDK:**

```bash
aws --endpoint-url http://localhost:9000 s3 ls s3://warehouse/ --recursive
# with credentials:
#   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=local
```

Use path-style addressing (`--endpoint-url` handles this for the AWS CLI).
Scoped keys `trino` / `lakekeeper` also work and are what the services use.

### Lakekeeper (Iceberg REST catalog)

**Management API** — no auth (`AUTHZ_BACKEND=allowall`):

```bash
curl -s http://localhost:8181/management/v1/warehouse | jq
curl -s "http://localhost:8181/catalog/v1/config?warehouse=DataLake" | jq
```

**As an Iceberg REST catalog** from PyIceberg / Spark / another Trino:

```
URI:       http://localhost:8181/catalog
Warehouse: DataLake
```

Web UI (warehouses, namespaces, tables): http://localhost:8181.

### Hive Metastore

Thrift service on `localhost:9083`, no web UI. In practice you only reach it
through Trino's `s3` catalog. To point another tool at it, use
`thrift://localhost:9083` and give that tool its own S3 credentials for MinIO.

---

## Using the catalogs

### `iceberg` — managed Iceberg tables (Lakekeeper)

The `lakekeeper_prepare` job creates a warehouse named `DataLake` and a namespace
that Trino sees as the schema `iceberg.datalake`.

```sql
CREATE TABLE iceberg.datalake.events (
  id        bigint,
  user_name varchar,
  ts        timestamp(6)
);

INSERT INTO iceberg.datalake.events VALUES
  (1, 'alice', TIMESTAMP '2026-01-01 10:00:00'),
  (2, 'bob',   TIMESTAMP '2026-01-01 11:30:00');

SELECT * FROM iceberg.datalake.events;

-- Iceberg features work: time travel, snapshots, schema evolution
SELECT * FROM iceberg.datalake."events$snapshots";
ALTER TABLE iceberg.datalake.events ADD COLUMN source varchar;
```

Data and metadata land under `s3://warehouse/datalake-warehouse/` in MinIO.

### `postgresql` — the PostgreSQL database

```sql
SHOW SCHEMAS FROM postgresql;
SELECT * FROM postgresql.public.some_table;
```

Read/write, subject to the `admin` role's privileges.

### `s3` — raw files on MinIO (Hive connector)

Use this to put SQL over Parquet/ORC files that already exist in MinIO, or to
write plain files that other tools can read. You register a schema (a directory)
and then tables (subdirectories) under it.

```sql
CREATE SCHEMA IF NOT EXISTS s3.raw
WITH (location = 's3://warehouse/raw/');

CREATE TABLE s3.raw.pageviews (
  path   varchar,
  views  bigint
)
WITH (
  external_location = 's3://warehouse/raw/pageviews/',
  format = 'PARQUET'
);

INSERT INTO s3.raw.pageviews VALUES ('/home', 42), ('/about', 7);
SELECT * FROM s3.raw.pageviews;
```

Notes:
- `format` can be `PARQUET`, `ORC`, `JSON`, `CSV`, `TEXTFILE`, …
- `external_location` tables are *not* deleted from MinIO when you `DROP TABLE`.
- To query files a different tool wrote, point `external_location` at their
  directory and declare a matching schema.

### Federated query — join across catalogs

```sql
SELECT
  e.user_name,
  p.views
FROM iceberg.datalake.events e
JOIN s3.raw.pageviews p ON p.path = '/home'
JOIN postgresql.public.users u ON u.id = e.id;
```

---

## Common operations

```bash
# Logs for one service
docker compose logs -f trino-coordinator

# Re-run the Lakekeeper bootstrap (safe; it is idempotent)
docker compose up -d --force-recreate lakekeeper_prepare

# Rebuild the Hive Metastore image after editing hive/
docker compose up -d --build hive-metastore

# Run a single query without an interactive shell
docker compose exec trino-coordinator trino --execute "SHOW CATALOGS"

# psql into PostgreSQL
docker compose exec postgres psql -U admin -d postgres
```

---

## Configuration layout

```
.env.example                     Template for .env (copy it; .env is git-ignored)
docker-compose.yml               All service definitions and the shared network

postgres/
  init-user-db.sh                Runs on first Postgres boot: creates the
                                 `lakekeeper` and `hive` roles + the `lakekeeper`
                                 and `metastore` databases

lakekeeper/
  bootstrap-lk.py                Runs once after Lakekeeper is healthy: bootstraps
                                 it and creates the `DataLake` warehouse + namespace

hive/
  Dockerfile                     apache/hive:4.0.0 + PostgreSQL JDBC driver +
                                 hadoop-aws on the classpath
  conf/core-site.xml             S3A settings for the metastore (endpoint,
                                 path-style, env-var credentials, s3:// scheme alias)

trino/etc/
  config.properties              Coordinator config (mounted as /etc/trino/config.properties)
  config.properties.worker       Worker config (mounted over the same path on the worker)
  node.properties                node.environment, data dir
  jvm.config                     JVM flags (2 GB heap cap for laptops)
  log.properties                 Log levels
  catalog/
    iceberg.properties           Iceberg REST connector -> Lakekeeper
    postgresql.properties         PostgreSQL connector
    s3.properties                Hive connector -> Hive Metastore
```

Trino reads `${ENV:VAR}` in the catalog files; the coordinator and worker get
those vars via `env_file: .env`.

---

## Security notes

This stack is for **local development only**:

- `.env` holds plaintext credentials. It is git-ignored; `.env.example` ships the
  weak defaults so the stack runs out of the box.
- `postgresql.properties` and the Lakekeeper `PG_ENCRYPTION_KEY` still contain
  literal dev values — fine locally, not for anything shared.
- Lakekeeper runs with `LAKEKEEPER__AUTHZ_BACKEND=allowall` (no authorization).
- MinIO's `warehouse` and `tmp` buckets are set to public/anonymous read.
- Ports 8080, 8181, 9000, 9001, 5432, 9083 are published to `localhost`.

Do not expose these ports beyond your machine.

---

## Troubleshooting

Common failures and fixes — image version pinning, the Hive Metastore's S3
access, a missing `.env`, `s3://` vs `s3a://` — are in
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

---

## References

- [Trino documentation](https://trino.io/docs/current/)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Trino Hive connector](https://trino.io/docs/current/connector/hive.html)
- [Lakekeeper](https://github.com/lakekeeper/lakekeeper)
- [Apache Hive Standalone Metastore](https://hive.apache.org/development/quickstart/)
- [MinIO documentation](https://min.io/docs/minio/container/index.html)
