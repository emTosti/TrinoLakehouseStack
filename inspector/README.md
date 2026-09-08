# schema-proposer (data inspector)

Watches the **`landing`** bucket. When an object appears, it samples a bounded
slice, guesses a schema, and writes a **proposal** (Markdown + JSON) to the
**`inspection-reports`** bucket. It never registers anything — a human reads the
proposal and applies the DDL by hand if the data is trusted.

## Why it exists

Uploads to `landing` are treated as potentially hostile. This service lets you
see *what* landed and *what schema it might have* without any automated system
touching the catalog or the query engine.

## Security model

| Control | How |
|---|---|
| No path to the catalog/engine | Own Docker network (`inspector-network`); only MinIO is reachable. No credentials for Postgres, Lakekeeper, Hive, Trino. |
| Least-privilege storage | MinIO key scoped to **read `landing`**, **write `inspection-reports`**. Cannot write `landing`, cannot touch `warehouse`. |
| No registration | Emits documents only. Applying a proposal is a separate manual step. |
| Bounded reads | Range GETs only — `MAX_DOWNLOAD_BYTES` (128 MiB) for Parquet, `MAX_TEXT_BYTES` (8 MiB) for text; `MAX_SAMPLE_ROWS` (1000). |
| Sandboxed parsing | Each file is parsed in a fresh `spawn` subprocess with `RLIMIT_CPU` + `RLIMIT_FSIZE` and a wall-clock kill. A hang/blow-up yields a "could not inspect safely" proposal instead of taking the service down. |
| Hardened container | `read_only` rootfs + tmpfs `/tmp`, `cap_drop: ALL`, `no-new-privileges`, non-root uid 10001, `mem_limit`, `pids_limit`, no published ports. |
| Output hygiene | Example values truncated (`EXAMPLE_MAX_LEN`) and control-char-escaped. |

Worst case if the container is fully compromised: the attacker can read the
`landing` bucket (data they uploaded) and write junk to `inspection-reports`.
They cannot reach real data or any other service.

## How it works

```
upload -> landing bucket
            |  MinIO s3:ObjectCreated webhook (auth token)
            v
   POST /minio-events   (this service, :8000)
            |  ranged GET of a bounded slice -> /tmp
            v
   spawn subprocess: detect format, infer schema, sample rows, flag anomalies
            |
            v
   inspection-reports/<dataset>/<timestamp>.md   (+ .json, + latest.md)
```

**Formats:** Parquet (schema from the footer, authoritative), CSV/TSV and
JSON/NDJSON (inferred from a sample, pure-Python). gzip is transparently
handled. ORC, Avro, zstd, anything else → flagged "unsupported, manual review".

**De-duplication:** a `_state/` marker per `(key, etag)` plus an hourly
per-directory debounce, so a burst of files in one prefix produces one report,
not fifty.

## Configuration (environment)

| Var | Default | Meaning |
|---|---|---|
| `S3_ENDPOINT` | `http://minio:9000` | MinIO S3 API |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | — | the scoped `inspector` key |
| `LANDING_BUCKET` | `landing` | bucket to watch |
| `REPORTS_BUCKET` | `inspection-reports` | where proposals go |
| `WEBHOOK_TOKEN` | — | must match MinIO's `MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_*` |
| `MAX_DOWNLOAD_BYTES` | `134217728` | Parquet files above this are not inspected |
| `MAX_TEXT_BYTES` | `8388608` | CSV/JSON sample size |
| `MAX_SAMPLE_ROWS` | `1000` | rows sampled for type inference and examples |
| `MAX_COLUMNS` | `512` | columns beyond this are dropped with a warning |
| `INSPECT_TIMEOUT_SECONDS` | `60` | per-file parsing budget (CPU + wall clock) |
| `INSPECT_MEMORY_MB` | `0` | `RLIMIT_AS` for the parser subprocess in MiB; `0` = rely on the container `mem_limit` (pyarrow's virtual footprint makes a hard cap unreliable) |
| `LISTEN_PORT` | `8000` | webhook listener port |
| `WORKERS` | `2` | concurrent inspections |

## Reading proposals

```bash
mc alias set datalake http://localhost:9000 minioadmin minioadmin
mc cat datalake/inspection-reports/<dataset>/latest.md
# or browse http://localhost:9001 -> inspection-reports bucket
```

Each proposal contains: detected format, per-column proposed type (+ alternates,
null %, distinct estimate, min/max, examples), a row sample, anomaly flags, a
confidence rating, and **ready-to-paste DDL** for both a managed
`iceberg.datalake.<table>` and an external `s3.landing.<table>`. Nothing runs
until you run it.

## Applying a proposal

There is deliberately no "apply" button. Once you've decided the data is safe:

```bash
docker compose exec trino-coordinator trino
```

then paste the DDL from the proposal, adjusting types as needed.
