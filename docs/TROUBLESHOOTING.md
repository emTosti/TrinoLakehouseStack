# Troubleshooting

Ordered roughly by how often you'll hit them. For each: the symptom, why it
happens, and the fix.

---

## `docker compose up` fails immediately: `env file .env not found`

**Why.** `postgres`, `trino-coordinator` and `trino-worker` declare
`env_file: .env`. Compose requires the file to exist.

**Fix.**

```bash
cp .env.example .env
```

---

## Trino queries fail with `s3.aws-access-key` / `S3_TRINO_USER` errors, or the catalog won't load

**Why.** The catalog `.properties` files use `${ENV:S3_TRINO_USER}` /
`${ENV:S3_TRINO_PASSWORD}`. Those come from `.env` via `env_file:`. If `.env` is
missing a key, or you started the containers before creating `.env`, the
substitution is empty.

**Fix.** Make sure `.env` has `S3_TRINO_USER` and `S3_TRINO_PASSWORD`, then:

```bash
docker compose up -d --force-recreate trino-coordinator trino-worker
```

---

## `postgres` container is unhealthy; logs mention *"in 18+, these Docker images are configured to store database data..."*

**Why.** `postgres:alpine` now resolves to PostgreSQL 18+. Those images
hard-error when a volume is bind-mounted directly at
`/var/lib/postgresql/data` — which is exactly what this repo does with
`./volume/db-data`.

**Fix.** The compose file pins `postgres:17-alpine` for this reason. If you
changed it, either put it back, or migrate to the 18+ layout (mount at
`/var/lib/postgresql` instead and let Postgres manage the version subdirectory).

If you switched images on an existing `./volume/db-data`, wipe it:

```bash
docker compose down
rm -rf volume/db-data
docker compose up -d
```

---

## Lakekeeper image is ARM-only

**Why.** `docker-compose.yml` pins `quay.io/lakekeeper/catalog:v0-arm64`. On an
x86_64 host it will fail to pull or run under emulation.

**Fix.** Change the tag (used via a YAML anchor, so one edit covers both the
`lakekeeper` and `migrate` services):

```yaml
  lakekeeper:
    image: &lakekeeper-image quay.io/lakekeeper/catalog:latest-amd64
```

Check the [Lakekeeper releases](https://github.com/lakekeeper/lakekeeper/pkgs/container/catalog)
for the current tag naming.

---

## `hive-metastore` exits 1 on first start: *"Failed to get schema version... the database system is starting up"*

**Why.** Postgres restarts internally right after running its first-boot init
scripts. The metastore's schema-init step can land in that brief window.

**Fix.** Usually none needed — the service has `restart: on-failure` and the
retry succeeds (schema init is idempotent). If it keeps failing, confirm the
`metastore` database and `hive` role exist:

```bash
docker compose exec postgres psql -U admin -d postgres -c "\l metastore"
docker compose exec postgres psql -U admin -d postgres -c "\du hive"
```

If they don't exist, the init script never ran (non-empty `./volume/db-data` on
first boot). Wipe it and start over.

---

## `CREATE SCHEMA s3.<name>` fails: *"Failed to create external path s3://... : null"*

**Why.** The Hive Metastore couldn't write the directory in MinIO. Two possible
causes:

1. `hadoop-aws` / the AWS SDK aren't on the metastore classpath.
2. The location URI uses the `s3://` scheme, which modern Hadoop doesn't register
   (only `s3a://`).

**Fix.** Both are handled in the repo — `hive/Dockerfile` copies the jars onto
the classpath, and `hive/conf/core-site.xml` aliases `fs.s3.impl` /
`fs.s3n.impl` to `S3AFileSystem`. If you edited either, rebuild:

```bash
docker compose up -d --build hive-metastore
```

Verify S3A works from inside the container:

```bash
docker compose exec hive-metastore bash -lc '
  export HADOOP_CLASSPATH=$(ls /opt/hive/lib/hadoop-aws-*.jar):$(ls /opt/hive/lib/aws-java-sdk-bundle-*.jar)
  /opt/hadoop/bin/hadoop fs -Dfs.s3a.endpoint=http://minio:9000 -Dfs.s3a.path.style.access=true -ls s3a://warehouse/'
```

---

## Trino coordinator exits 100 with *"Configuration property ... was not used"* for an Iceberg or S3 key

**Why.** The catalog `.properties` file uses property names from an older Trino
release. The image is `trinodb/trino:latest` and connector config keys drift
between versions. Recent breaks seen:

| Old | Current |
|---|---|
| `iceberg.catalog.uri` | `iceberg.rest-catalog.uri` |
| `iceberg.catalog.warehouse` | `iceberg.rest-catalog.warehouse` |
| `iceberg.catalog.io-impl=...S3FileIO` | *(remove — native S3 filesystem)* |
| `s3.access-key` / `s3.secret-key` | `s3.aws-access-key` / `s3.aws-secret-key` (+ `fs.native-s3.enabled=true`) |

**Fix.** Update the catalog file to match the running Trino version (the exact
version is in the coordinator log: `Java version` line is followed by
`Trino <version>`). Consider pinning `trinodb/trino:<version>` instead of
`:latest` so this stops moving.

---

## Trino coordinator exits with *"No enum constant io.airlift.log.Level.WARNING"*

**Why.** `trino/etc/log.properties` used the level `WARNING`. Trino's valid
levels are `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG`.

**Fix.** Use `WARN`, not `WARNING`.

---

## Trino coordinator exits 127: *"/usr/lib/trino/bin/launcher.sh: No such file or directory"*

**Why.** An old custom `command:` in `docker-compose.yml` called
`launcher.sh`. Current images use `/usr/lib/trino/bin/run-trino` (the default
entrypoint) and ship config at `/etc/trino/`.

**Fix.** Remove the `command:` override and mount a `config.properties` file
instead — which is what the repo now does.

---

## `node.data_dir` warnings / Trino ignores the data dir

**Why.** The current key is `node.data-dir` (hyphen). `node.data_dir`
(underscore) is silently ignored.

**Fix.** Use `node.data-dir` in `trino/etc/node.properties`.

---

## Changed a password in `.env` but the service still uses the old one

**Why.** `postgres/init-user-db.sh` and `lakekeeper/bootstrap-lk.py` only run
once, against empty state. Roles, databases and the Lakekeeper warehouse are
already created with the old values.

**Fix.** Full reset:

```bash
docker compose down
rm -rf volume/
docker compose up -d
```

---

## `SHOW SCHEMAS FROM iceberg` is empty / `iceberg.datalake` doesn't exist

**Why.** `lakekeeper_prepare` didn't finish. Check its log:

```bash
docker compose logs lakekeeper_prepare
```

Common cause: an invalid field in the warehouse storage profile in
`bootstrap-lk.py` gets a `400` from Lakekeeper, which the script misreports as
"already exists" before failing later on `KeyError: 'defaults'`.

**Fix.** Read the actual API response, correct the payload, then re-run:

```bash
docker compose up -d --force-recreate lakekeeper_prepare
```

---

## Everything is wedged — start clean

```bash
docker compose down --remove-orphans
rm -rf volume/
docker rmi data-lake-trino-stack/hive-metastore:4.0.0   # force a rebuild
docker compose up -d --build
```
