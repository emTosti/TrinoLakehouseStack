# Architecture

## The idea

Trino is a **query engine, not a database**. It stores nothing itself. Every
table it can see belongs to a *catalog*, and each catalog is a connector pointed
at some external system. A single SQL statement can touch several catalogs at
once, which is what makes "federated" queries possible.

This stack wires up three catalogs:

| Catalog | Connector | Metadata source | Data source |
|---|---|---|---|
| `iceberg` | `iceberg` (REST) | Lakekeeper | MinIO |
| `s3` | `hive` | Hive Metastore | MinIO |
| `postgresql` | `postgresql` | — (JDBC) | PostgreSQL |

## Components and why each one exists

### MinIO — object storage

S3-compatible storage running locally. Holds every data file: Iceberg data and
manifest files, and the raw Parquet/ORC files behind the `s3` catalog.

Buckets, created by the `prepare_buckets` (`minio-setup`) job:

| Bucket | Purpose |
|---|---|
| `warehouse` | Iceberg warehouse (`datalake-warehouse/` prefix) and raw `s3` catalog files |
| `trino` | Spill / scratch space for Trino |
| `tmp` | General scratch |

The same job also creates two scoped access keys — `trino` and `lakekeeper` —
each with a `readwrite` policy, so Trino and Lakekeeper don't use the MinIO root
account.

### PostgreSQL — metadata store *and* a query source

One Postgres instance serves three roles:

1. **Lakekeeper's** catalog database (`lakekeeper`).
2. **The Hive Metastore's** schema database (`metastore`).
3. **A Trino data source** — the `postgresql` catalog points at the default
   `postgres` database.

`postgres/init-user-db.sh` runs on the *first* boot only (empty data directory).
It creates the `lakekeeper` and `hive` login roles and their databases. If you
change credentials in `.env` after the first boot, you must
`rm -rf volume/db-data` (or `docker compose down` + `rm -rf volume/`) for the
script to run again.

Postgres is pinned to `postgres:17-alpine`. The 18+ images refuse to start when a
volume is bind-mounted at `/var/lib/postgresql/data`, which is this repo's
`./volume/db-data` convention — see TROUBLESHOOTING.

### Lakekeeper — Iceberg REST catalog

Implements the [Iceberg REST catalog](https://iceberg.apache.org/concepts/catalog/)
API. Trino's `iceberg` connector talks to it at
`http://lakekeeper:8181/catalog`. Lakekeeper tracks table pointers, snapshots and
schemas in Postgres, and knows the data lives under `s3://warehouse/` in MinIO.

Startup sequence:

1. `migrate` (`lakekeeper_migrate`) — runs the DB schema migrations, then exits.
2. `lakekeeper` — the server; waits for `migrate` to finish and Postgres/MinIO to
   be healthy.
3. `lakekeeper_prepare` — a one-shot Python job (`lakekeeper/bootstrap-lk.py`)
   that:
   - calls `POST /management/v1/bootstrap` (accepts terms, initializes the
     instance),
   - creates a warehouse named **`DataLake`** with an S3 storage profile
     (`bucket=warehouse`, `key-prefix=datalake-warehouse`, path-style, STS
     enabled, `s3-compat` flavor),
   - creates a namespace `["DataLake"]`, which Trino exposes as the schema
     `iceberg.datalake`.

   It is idempotent — re-running it is safe (bootstrap returns 400 "already
   done", warehouse/namespace creation returns 409).

Lakekeeper runs with `AUTHZ_BACKEND=allowall`: no authorization checks.

### Hive Metastore — for the `s3` catalog

Trino's `iceberg` connector has its own catalog (Lakekeeper), but the `hive`
connector needs a **Hive Metastore** to record schemas and table definitions for
plain files. There is no Hive *server* here — only the standalone metastore
(Thrift on port 9083).

`hive/Dockerfile` builds on `apache/hive:4.0.0` with two additions:

1. **PostgreSQL JDBC driver** (`postgresql-42.7.4.jar`) — the base image ships
   only a Derby driver; the metastore keeps its schema in the `metastore`
   Postgres database.
2. **`hadoop-aws` + the AWS SDK bundle** copied onto the metastore classpath.
   Both jars already exist in the image under
   `/opt/hadoop/share/hadoop/tools/lib`, which is *not* on the default classpath.
   Without them the metastore cannot touch MinIO at all.

`hive/conf/core-site.xml` is mounted via `HIVE_CUSTOM_CONF_DIR` and configures
S3A:

- `fs.s3a.endpoint = http://minio:9000`, path-style access, TLS off.
- Credentials via `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` —
  the metastore reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from its
  environment (set to the `trino` MinIO key in `docker-compose.yml`). No secrets
  in the XML file.
- `fs.s3.impl` / `fs.s3n.impl` aliased to `S3AFileSystem`. Trino writes location
  URIs with the `s3://` scheme; modern Hadoop only registers `s3a://`, so the
  metastore needs the alias to create schema/table directories. `S3AFileSystem`
  keeps reading its `fs.s3a.*` settings regardless of the scheme it is mounted
  under.

The metastore has `restart: on-failure` because Postgres performs an internal
restart immediately after running its first-boot init scripts; the metastore's
schema-init step can hit that window and fail once. The retry succeeds (schema
init is idempotent).

### Trino — coordinator + worker

- **`trino-coordinator`** (port 8080) — parses and plans queries, hosts the UI
  and the discovery service, and also runs tasks
  (`node-scheduler.include-coordinator=true`).
- **`trino-worker`** — one additional worker for distributed execution.

Both mount the same `trino/etc/catalog/` directory. They get different
`config.properties` files (`config.properties` vs `config.properties.worker`)
mounted onto the same in-container path. The stock image entrypoint
(`/usr/lib/trino/bin/run-trino`) starts the server; there is no custom `command`.

Catalog files use Trino's `${ENV:VAR}` substitution for the MinIO keys; the
coordinator and worker receive those variables through `env_file: .env`.

## Request flow: `SELECT` against an Iceberg table

1. Client sends SQL to the coordinator (8080).
2. Coordinator asks the `iceberg` catalog (Lakekeeper REST) for the table's
   current metadata location.
3. Lakekeeper returns a pointer to a metadata JSON file in
   `s3://warehouse/datalake-warehouse/...`.
4. Coordinator reads the metadata and manifests from MinIO, builds a plan, and
   splits the work.
5. Coordinator and worker read the Parquet data files from MinIO in parallel
   using Trino's native S3 client (configured in `iceberg.properties`).
6. Results stream back through the coordinator to the client.

The Hive Metastore and Lakekeeper are only consulted for *metadata*. Bulk data
never flows through them.

## Startup dependency graph

```
minio ─┬─> prepare_buckets (minio-setup)
       │
postgres ─┬─> migrate (lakekeeper_migrate) ─> lakekeeper ─> lakekeeper_prepare
          │
          └─> hive-metastore

minio + postgres + lakekeeper + hive-metastore  ──(all healthy)──>  trino-coordinator ──> trino-worker

schema-proposer  <──(webhook)──  minio     (isolated on inspector-network)
```

## schema-proposer — inspecting untrusted uploads

A separate, deliberately isolated service. It does **not** register data — it
watches a bucket of untrusted uploads and produces a schema *proposal* for a
human to review.

- **`landing` bucket** — where untrusted files are uploaded. Physically separate
  from `warehouse`. MinIO fires an `s3:ObjectCreated` webhook (with an auth
  token) at the inspector.
- **`schema-proposer` service** — receives the webhook, does a *bounded* ranged
  GET of the new object, and parses it in a short-lived `spawn` subprocess with
  `RLIMIT_CPU` + a wall-clock kill. It infers a schema (Parquet from the footer;
  CSV/JSON from a sample), collects example values and anomaly flags, and writes
  a Markdown + JSON proposal — including ready-to-paste DDL — to the
  **`inspection-reports` bucket**.
- **Isolation.** The service sits on its own `inspector-network` with only MinIO
  reachable. Its MinIO key can read `landing` and write `inspection-reports` and
  nothing else. It has no credentials or network route to Postgres, Lakekeeper,
  the Hive Metastore, or Trino. The container runs read-only, non-root, with all
  capabilities dropped and `no-new-privileges`.
- **Nothing is applied.** A person reads the proposal and, if the data is
  trusted, runs the DDL by hand.

This fills the "new / unknown data" gap: raw files never become queryable tables
automatically, but you get a reviewed, low-risk on-ramp for the ones that
should. Full detail: [../inspector/README.md](../inspector/README.md).
