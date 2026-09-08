"""Schema-proposer / data inspector.

Flow
----
1. MinIO fires an ``s3:ObjectCreated`` webhook at ``POST /minio-events`` when an
   object lands in the ``landing`` bucket.
2. For each new object we download a *bounded* slice (never the whole file for
   large inputs), then parse it in a short-lived subprocess with CPU/time limits.
3. We infer a schema, collect example values and a small row sample, note any
   anomalies, and render a proposal (Markdown + JSON).
4. The proposal is written to the ``inspection-reports`` bucket. Nothing is
   registered anywhere -- a human reviews the proposal and applies the DDL by
   hand if the data is trusted.

Security model
--------------
* The container has network access to MinIO only, and MinIO credentials scoped
  to: read ``landing``, write ``inspection-reports``. Nothing else.
* Reads are range-limited; parsing is sandboxed (RLIMIT_CPU + wall-clock kill).
* A file that hangs or blows up the parser yields a "could not inspect safely"
  proposal rather than taking the service down.
"""

from __future__ import annotations

import base64
import concurrent.futures
import dataclasses
import datetime as dt
import hashlib
import io
import json
import multiprocessing as mp
import os
import re
import resource
import string
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import unquote_plus

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

# --------------------------------------------------------------------------- #
# Configuration (all from the environment)
# --------------------------------------------------------------------------- #

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_REGION = os.environ.get("S3_REGION", "local")

LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "landing")
REPORTS_BUCKET = os.environ.get("REPORTS_BUCKET", "inspection-reports")

WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8000"))

MAX_DOWNLOAD_BYTES = int(os.environ.get("MAX_DOWNLOAD_BYTES", str(128 * 1024 * 1024)))
MAX_TEXT_BYTES = int(os.environ.get("MAX_TEXT_BYTES", str(8 * 1024 * 1024)))
MAX_SAMPLE_ROWS = int(os.environ.get("MAX_SAMPLE_ROWS", "1000"))
MAX_COLUMNS = int(os.environ.get("MAX_COLUMNS", "512"))
EXAMPLE_MAX_LEN = int(os.environ.get("EXAMPLE_MAX_LEN", "120"))

INSPECT_TIMEOUT_SECONDS = int(os.environ.get("INSPECT_TIMEOUT_SECONDS", "60"))
# RLIMIT_AS in MiB for the parsing subprocess; 0 disables it (rely on the
# container mem_limit instead -- pyarrow's virtual footprint makes a hard
# address-space cap unreliable).
INSPECT_MEMORY_MB = int(os.environ.get("INSPECT_MEMORY_MB", "0"))

WORKERS = int(os.environ.get("WORKERS", "2"))

NULL_TOKENS = {"", "null", "NULL", "Null", "na", "NA", "N/A", "n/a", r"\N", "nan", "NaN", "None"}

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?([Zz]|[+-]\d{2}:?\d{2})?$")
INT_RE = re.compile(r"^[+-]?\d{1,19}$")
FLOAT_RE = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
BOOL_TOKENS = {"true", "false", "t", "f", "yes", "no"}


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=10,
            read_timeout=30,
        ),
    )


# --------------------------------------------------------------------------- #
# Type inference helpers (text formats -- pure Python, bounded)
# --------------------------------------------------------------------------- #

def _classify_scalar(value: str) -> str:
    """Return one of: int, float, bool, date, timestamp, string."""
    if INT_RE.match(value):
        # keep it in int64 range
        try:
            int(value)
            return "int"
        except ValueError:
            return "string"
    if FLOAT_RE.match(value):
        return "float"
    low = value.lower()
    if low in BOOL_TOKENS:
        return "bool"
    if ISO_DATE_RE.match(value):
        return "date"
    if ISO_TS_RE.match(value):
        return "timestamp"
    return "string"


_KIND_TO_TRINO = {
    "int": "BIGINT",
    "float": "DOUBLE",
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP(6)",
    "string": "VARCHAR",
}


def _resolve_column_type(kinds: dict[str, int]) -> tuple[str, list[str]]:
    """Collapse a count of observed scalar kinds into one Trino type + alternates."""
    total = sum(kinds.values())
    if total == 0:
        return "VARCHAR", []
    present = set(kinds)
    if present <= {"int", "float"}:
        return ("BIGINT" if present == {"int"} else "DOUBLE"), []
    if present == {"date"}:
        return "DATE", []
    if present <= {"date", "timestamp"}:
        return "TIMESTAMP(6)", (["DATE"] if "date" in kinds else [])
    if present == {"bool"}:
        return "BOOLEAN", []

    ordered = [k for k, _ in sorted(kinds.items(), key=lambda kv: kv[1], reverse=True)]
    dominant = ordered[0]
    if kinds[dominant] / total >= 0.95:
        return _KIND_TO_TRINO.get(dominant, "VARCHAR"), [_KIND_TO_TRINO.get(k, "VARCHAR") for k in ordered[1:]]
    # genuinely mixed -> VARCHAR, but record what else was seen
    return "VARCHAR", [_KIND_TO_TRINO.get(k, "VARCHAR") for k in ordered if k != "string"][:3]


def _clean_example(value: Any) -> str:
    s = value if isinstance(value, str) else json.dumps(value, default=str, ensure_ascii=False)
    s = "".join(ch if ch in string.printable and ch not in "\r\n\t\x0b\x0c" else "." for ch in s)
    if len(s) > EXAMPLE_MAX_LEN:
        s = s[:EXAMPLE_MAX_LEN] + "..."
    return s


# --------------------------------------------------------------------------- #
# Column / proposal data model
# --------------------------------------------------------------------------- #

@dataclasses.dataclass
class Column:
    name: str
    trino_type: str
    alt_types: list[str] = dataclasses.field(default_factory=list)
    null_pct: float = 0.0
    distinct_estimate: int | None = None
    minimum: str | None = None
    maximum: str | None = None
    examples: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Proposal:
    dataset: str
    source_key: str
    source_size: int
    detected_format: str
    detected_compression: str | None
    sampled_rows: int
    row_count_estimate: int | None
    confidence: str
    columns: list[Column]
    sample_rows: list[list[str]]
    warnings: list[str]
    error: str | None = None


# --------------------------------------------------------------------------- #
# Format detection
# --------------------------------------------------------------------------- #

def detect_format(key: str, head: bytes, tail: bytes) -> tuple[str, str | None]:
    """Return (format, compression). format in
    {parquet, csv, tsv, json, ndjson, unsupported}."""
    lower = key.lower()
    compression = None
    if head[:2] == b"\x1f\x8b":
        compression = "gzip"
    elif head[:4] == b"\x28\xb5\x2f\xfd":
        compression = "zstd"
    for suffix in (".gz", ".zst", ".zstd", ".bz2"):
        if lower.endswith(suffix):
            lower = lower[: -len(suffix)]
            break

    if head[:4] == b"PAR1" or tail[-4:] == b"PAR1" or lower.endswith(".parquet"):
        return "parquet", None
    if head[:3] == b"ORC" or lower.endswith(".orc"):
        return "unsupported", None
    if head[:4] == b"Obj\x01" or lower.endswith(".avro"):
        return "unsupported", None

    if lower.endswith((".ndjson", ".jsonl")):
        return "ndjson", compression
    if lower.endswith(".json"):
        return "json", compression
    if lower.endswith(".tsv"):
        return "tsv", compression
    if lower.endswith(".csv"):
        return "csv", compression

    # sniff text
    probe = head
    if compression == "gzip":
        try:
            import gzip
            probe = gzip.decompress(head + b"\x00" * 0)[:4096]
        except Exception:
            probe = b""
    stripped = probe.lstrip()
    if stripped[:1] in (b"{", b"["):
        # one object per line -> ndjson, else json
        first_line = stripped.split(b"\n", 1)[0]
        try:
            json.loads(first_line)
            return "ndjson", compression
        except Exception:
            return "json", compression
    if probe and b"," in probe.split(b"\n", 1)[0]:
        return "csv", compression
    if probe and b"\t" in probe.split(b"\n", 1)[0]:
        return "tsv", compression
    return "unsupported", compression


# --------------------------------------------------------------------------- #
# Inspectors -- run inside the sandboxed subprocess
# --------------------------------------------------------------------------- #

def inspect_parquet(path: str, full: bool) -> dict:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    md = pf.metadata
    schema = pf.schema_arrow
    warnings: list[str] = []
    if md.num_columns > MAX_COLUMNS:
        warnings.append(f"{md.num_columns} columns (> {MAX_COLUMNS}); truncated")
    names = schema.names[: min(md.num_columns, MAX_COLUMNS)]

    columns: list[Column] = []
    sample_rows: list[list[str]] = []
    sampled = 0
    pydata: dict[str, list] = {}

    if md.num_rows > 0:
        # iter_batches reads incrementally -- one bounded batch, not a whole row group
        try:
            batch = next(pf.iter_batches(batch_size=min(MAX_SAMPLE_ROWS, 5000), columns=names))
        except StopIteration:
            batch = None
        if batch is not None:
            batch = batch.slice(0, MAX_SAMPLE_ROWS)
            sampled = batch.num_rows
            pydata = {name: batch.column(name).to_pylist() for name in names}

    for name in names:
        field = schema.field(name)
        vals = pydata.get(name, [])
        non_null = [v for v in vals if v is not None]
        col = Column(name=name, trino_type=_arrow_to_trino(field.type))
        if vals:
            col.null_pct = round(100 * (len(vals) - len(non_null)) / len(vals), 1)
            col.distinct_estimate = len(set(map(str, non_null))) or None
            col.examples = [_clean_example(v) for v in list(dict.fromkeys(map(_json_safe, non_null)))[:5]]
            if non_null and _arrow_is_ordered(field.type):
                try:
                    col.minimum = _clean_example(_json_safe(min(non_null)))
                    col.maximum = _clean_example(_json_safe(max(non_null)))
                except TypeError:
                    pass
        columns.append(col)

    for i in range(min(sampled, 20)):
        sample_rows.append([_clean_example(_json_safe(pydata[n][i])) for n in names])

    if not full:
        warnings.append("file exceeds the safe-download limit; schema only, no samples")

    return dict(
        detected_format="parquet",
        detected_compression=None,
        confidence="high" if (full and sampled) else "medium",
        row_count_estimate=md.num_rows,
        sampled_rows=sampled,
        columns=[dataclasses.asdict(c) for c in columns],
        sample_rows=sample_rows,
        warnings=warnings,
    )


def _json_safe(v: Any) -> Any:
    if isinstance(v, (dt.date, dt.datetime, dt.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return base64.b64encode(v[:64]).decode() + ("..." if len(v) > 64 else "")
    return v


def _arrow_to_trino(t) -> str:
    import pyarrow as pa

    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_integer(t):
        return "INTEGER" if t.bit_width <= 32 else "BIGINT"
    if pa.types.is_floating(t):
        return "REAL" if t.bit_width == 32 else "DOUBLE"
    if pa.types.is_decimal(t):
        return f"DECIMAL({t.precision},{t.scale})"
    if pa.types.is_date(t):
        return "DATE"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP(6)"
    if pa.types.is_time(t):
        return "TIME(6)"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "VARCHAR"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "VARBINARY"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return f"ARRAY({_arrow_to_trino(t.value_type)})"
    if pa.types.is_struct(t):
        return "ROW(" + ", ".join(f"{f.name} {_arrow_to_trino(f.type)}" for f in t) + ")"
    if pa.types.is_map(t):
        return f"MAP({_arrow_to_trino(t.key_type)}, {_arrow_to_trino(t.item_type)})"
    return "VARCHAR"


def _arrow_is_ordered(t) -> bool:
    import pyarrow as pa

    return (
        pa.types.is_integer(t)
        or pa.types.is_floating(t)
        or pa.types.is_decimal(t)
        or pa.types.is_date(t)
        or pa.types.is_timestamp(t)
        or pa.types.is_string(t)
    )


def inspect_text(path: str, fmt: str, compression: str | None) -> dict:
    if compression and compression != "gzip":
        return _empty(fmt, [f"{compression}-compressed; v1 only decompresses gzip"])
    raw = open(path, "rb").read(MAX_TEXT_BYTES + 1)
    truncated = len(raw) > MAX_TEXT_BYTES
    raw = raw[:MAX_TEXT_BYTES]
    if compression == "gzip":
        import gzip

        try:
            raw = gzip.decompress(raw)
        except Exception:
            # partial gzip stream -- decompress what we can
            d = gzip.GzipFile(fileobj=io.BytesIO(raw))
            buf = b""
            try:
                while chunk := d.read(65536):
                    buf += chunk
                    if len(buf) > MAX_TEXT_BYTES:
                        break
            except Exception:
                pass
            raw = buf
    text = raw.decode("utf-8", errors="replace")
    replacements = text.count("�")
    warnings: list[str] = []
    if truncated:
        warnings.append(f"sampled only the first {MAX_TEXT_BYTES // 1024} KiB")
    if replacements:
        warnings.append(f"{replacements} invalid UTF-8 byte(s) replaced")

    if fmt in ("json", "ndjson"):
        return _inspect_json(text, fmt, warnings)
    return _inspect_delimited(text, "\t" if fmt == "tsv" else ",", warnings)


def _inspect_delimited(text: str, delim: str, warnings: list[str]) -> dict:
    import csv

    lines = text.splitlines()
    if not lines:
        return _empty("csv", warnings + ["file is empty"])
    reader = csv.reader(lines, delimiter=delim)
    rows = []
    for i, row in enumerate(reader):
        if i > MAX_SAMPLE_ROWS:
            break
        rows.append(row)
    if not rows:
        return _empty("csv", warnings + ["no rows parsed"])

    header = rows[0]
    has_header = all(c and not INT_RE.match(c) and not FLOAT_RE.match(c) for c in header)
    if has_header:
        names = _dedupe([_slug(c) or f"col_{i+1}" for i, c in enumerate(header)])
        data = rows[1:]
    else:
        names = [f"col_{i+1}" for i in range(len(header))]
        data = rows
    ncols = len(names)
    if ncols > MAX_COLUMNS:
        warnings.append(f"{ncols} columns (> {MAX_COLUMNS}); truncating")
        names = names[:MAX_COLUMNS]
        ncols = MAX_COLUMNS

    ragged = sum(1 for r in data if len(r) != ncols)
    if ragged:
        warnings.append(f"{ragged}/{len(data)} rows have an unexpected column count")

    columns: list[Column] = []
    for ci, name in enumerate(names):
        kinds: dict[str, int] = {}
        nulls = 0
        distinct: set[str] = set()
        examples: list[str] = []
        mn = mx = None
        for r in data:
            cell = r[ci] if ci < len(r) else ""
            if cell in NULL_TOKENS:
                nulls += 1
                continue
            k = _classify_scalar(cell)
            kinds[k] = kinds.get(k, 0) + 1
            if len(distinct) < 2048:
                distinct.add(cell)
            if len(examples) < 5 and cell not in examples:
                examples.append(_clean_example(cell))
            if k in ("int", "float", "date", "timestamp"):
                mn = cell if mn is None or cell < mn else mn
                mx = cell if mx is None or cell > mx else mx
        ttype, alts = _resolve_column_type(kinds)
        n = len(data) or 1
        columns.append(
            Column(
                name=name,
                trino_type=ttype,
                alt_types=alts,
                null_pct=round(100 * nulls / n, 1),
                distinct_estimate=len(distinct),
                minimum=_clean_example(mn) if mn is not None else None,
                maximum=_clean_example(mx) if mx is not None else None,
                examples=examples,
            )
        )

    sample = [[_clean_example(c) for c in r[:ncols]] for r in data[:20]]
    return dict(
        detected_format="csv" if delim == "," else "tsv",
        detected_compression=None,
        confidence="medium",
        row_count_estimate=None,
        sampled_rows=len(data),
        columns=[dataclasses.asdict(c) for c in columns],
        sample_rows=sample,
        warnings=warnings + ([] if has_header else ["no header row detected; column names are synthetic"]),
    )


def _inspect_json(text: str, fmt: str, warnings: list[str]) -> dict:
    records: list[dict] = []
    if fmt == "ndjson":
        for line in text.splitlines():
            if not line.strip():
                continue
            if len(records) >= MAX_SAMPLE_ROWS:
                break
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                warnings.append("one or more lines are not valid JSON")
                continue
            if isinstance(obj, dict):
                records.append(obj)
    else:
        try:
            doc = json.loads(text)
        except json.JSONDecodeError:
            return _empty("json", warnings + ["not valid JSON (or truncated by the sample limit)"])
        if isinstance(doc, list):
            records = [x for x in doc[:MAX_SAMPLE_ROWS] if isinstance(x, dict)]
        elif isinstance(doc, dict):
            records = [doc]

    if not records:
        return _empty("json", warnings + ["no JSON objects found to infer a schema from"])

    keys: list[str] = []
    seen = set()
    for rec in records:
        for k in rec:
            if k not in seen:
                seen.add(k)
                keys.append(k)
    if len(keys) > MAX_COLUMNS:
        warnings.append(f"{len(keys)} keys (> {MAX_COLUMNS}); truncating")
        keys = keys[:MAX_COLUMNS]

    columns: list[Column] = []
    for k in keys:
        kinds: dict[str, int] = {}
        nulls = 0
        present = 0
        examples: list[str] = []
        nested = False
        for rec in records:
            if k not in rec:
                continue
            present += 1
            v = rec[k]
            if v is None:
                nulls += 1
                continue
            jk = _json_kind(v)
            if jk in ("object", "array"):
                nested = True
            kinds[jk] = kinds.get(jk, 0) + 1
            if len(examples) < 5:
                examples.append(_clean_example(_json_safe(v)))
        ttype, alts = _resolve_json_type(kinds)
        missing = len(records) - present
        note_type = ttype
        columns.append(
            Column(
                name=_slug(k) or "field",
                trino_type=note_type,
                alt_types=alts + (["-- nested; consider flattening or JSON type"] if nested else []),
                null_pct=round(100 * (nulls + missing) / len(records), 1),
                distinct_estimate=None,
                examples=examples,
            )
        )

    sample = []
    for rec in records[:20]:
        sample.append([_clean_example(_json_safe(rec.get(k))) for k in keys])
    return dict(
        detected_format=fmt,
        detected_compression=None,
        confidence="medium" if fmt == "ndjson" else "low",
        row_count_estimate=None,
        sampled_rows=len(records),
        columns=[dataclasses.asdict(c) for c in columns],
        sample_rows=sample,
        warnings=warnings,
    )


def _json_kind(v: Any) -> str:
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return _classify_scalar(v) if _classify_scalar(v) in ("date", "timestamp") else "string"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "object"
    return "string"


def _resolve_json_type(kinds: dict[str, int]) -> tuple[str, list[str]]:
    if not kinds:
        return "VARCHAR", []
    ks = set(kinds)
    if ks == {"int"}:
        return "BIGINT", []
    if ks <= {"int", "float"}:
        return "DOUBLE", []
    if ks == {"bool"}:
        return "BOOLEAN", []
    if ks == {"date"}:
        return "DATE", []
    if ks <= {"date", "timestamp"}:
        return "TIMESTAMP(6)", []
    if ks == {"object"}:
        return "JSON", ["nested object — JSON, or flatten into ROW(...)"]
    if ks == {"array"}:
        return "JSON", ["array — JSON, or ARRAY(<element type>)"]
    if ks == {"string"}:
        return "VARCHAR", []
    if ks <= {"object", "array"}:
        return "JSON", ["mixed nested — JSON"]
    return "VARCHAR", sorted(_KIND_TO_TRINO.get(k, k.upper()) for k in kinds)


def _empty(fmt: str, warnings: list[str]) -> dict:
    return dict(
        detected_format=fmt,
        detected_compression=None,
        confidence="none",
        row_count_estimate=None,
        sampled_rows=0,
        columns=[],
        sample_rows=[],
        warnings=warnings,
    )


# --------------------------------------------------------------------------- #
# Identifier helpers
# --------------------------------------------------------------------------- #

def _slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9_]+", "_", name.strip().lower()).strip("_")
    s = re.sub(r"_+", "_", s)
    if s and s[0].isdigit():
        s = "c_" + s
    return s[:128]


def _dedupe(names: list[str]) -> list[str]:
    out: list[str] = []
    seen: dict[str, int] = {}
    for n in names:
        if n in seen:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 1
            out.append(n)
    return out


def _dataset_of(key: str) -> tuple[str, str]:
    """(dataset directory, proposed table name) from an object key."""
    parts = [p for p in key.split("/") if p]
    if len(parts) <= 1:
        name = re.sub(r"\.[a-z0-9]+$", "", parts[-1], flags=re.I) if parts else "dataset"
        return "", _slug(name) or "dataset"
    ddir = "/".join(parts[:-1])
    # table name: nearest ancestor segment that is not just digits (skip date parts)
    for seg in reversed(parts[:-1]):
        if seg and not seg.replace("-", "").replace("_", "").isdigit():
            return ddir, _slug(seg) or "dataset"
    return ddir, _slug(parts[0]) or "dataset"


# --------------------------------------------------------------------------- #
# Sandbox runner
# --------------------------------------------------------------------------- #

def _sandbox_target(fmt: str, path: str, full: bool, compression: str | None, q: mp.Queue) -> None:
    try:
        resource.setrlimit(resource.RLIMIT_CPU, (INSPECT_TIMEOUT_SECONDS, INSPECT_TIMEOUT_SECONDS + 5))
        resource.setrlimit(resource.RLIMIT_FSIZE, (256 * 1024 * 1024, 256 * 1024 * 1024))
        if INSPECT_MEMORY_MB > 0:
            cap = INSPECT_MEMORY_MB * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (cap, cap))

        if fmt == "parquet":
            result = inspect_parquet(path, full)
        else:
            result = inspect_text(path, fmt, compression)
        q.put(("ok", result))
    except BaseException as exc:  # noqa: BLE001 -- hostile input: catch everything, incl. MemoryError
        q.put(("error", f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}"))


def run_sandboxed(fmt: str, path: str, full: bool, compression: str | None) -> tuple[str, Any]:
    import queue as _queue

    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    proc = ctx.Process(target=_sandbox_target, args=(fmt, path, full, compression, q), daemon=True)
    proc.start()
    try:
        status, payload = q.get(timeout=INSPECT_TIMEOUT_SECONDS + 15)
    except _queue.Empty:
        status, payload = "error", "inspection produced no result in time -- treat this file as hostile"
    proc.join(5)
    if proc.is_alive():
        proc.terminate()
        proc.join(3)
        if proc.is_alive():
            proc.kill()
    return status, payload


# --------------------------------------------------------------------------- #
# Report rendering
# --------------------------------------------------------------------------- #

def build_proposal(key: str, size: int, result: dict, error: str | None) -> Proposal:
    ddir, _ = _dataset_of(key)
    cols = [Column(**c) for c in result.get("columns", [])]
    return Proposal(
        dataset=ddir or key,
        source_key=key,
        source_size=size,
        detected_format=result.get("detected_format", "unknown"),
        detected_compression=result.get("detected_compression"),
        sampled_rows=result.get("sampled_rows", 0),
        row_count_estimate=result.get("row_count_estimate"),
        confidence=result.get("confidence", "none"),
        columns=cols,
        sample_rows=result.get("sample_rows", []),
        warnings=result.get("warnings", []),
        error=error,
    )


def _ddl(p: Proposal) -> tuple[str, str]:
    ddir, table = _dataset_of(p.source_key)
    coldefs = ",\n  ".join(f'"{c.name}" {c.trino_type}' for c in p.columns) or "-- no columns inferred"
    iceberg = f"CREATE TABLE iceberg.datalake.{table} (\n  {coldefs}\n);"
    fmt = p.detected_format.upper()
    if fmt in ("NDJSON", "JSON"):
        fmt = "JSON"
    ddir = ddir.strip("/")
    loc = f"s3://{LANDING_BUCKET}/" + (ddir + "/" if ddir else "")
    root_note = "" if ddir else "  -- NOTE: object is at the bucket root; this location matches every root object\n"
    if ddir and any(seg.isdigit() for seg in ddir.split("/")):
        root_note += ("  -- NOTE: path looks date/partition-structured; consider pointing external_location\n"
                      "  --       at the dataset root instead and declaring partition columns\n")
    hive = (
        f"CREATE SCHEMA IF NOT EXISTS s3.landing WITH (location = 's3://{LANDING_BUCKET}/');\n"
        f"CREATE TABLE s3.landing.{table} (\n  {coldefs}\n)\n"
        f"{root_note}"
        f"WITH (external_location = '{loc}', format = '{fmt}');"
    )
    return iceberg, hive


def render_markdown(p: Proposal) -> str:
    lines: list[str] = []
    lines.append(f"# Schema proposal — `{p.dataset}`")
    lines.append("")
    lines.append("> **Nothing has been registered.** This is a proposal generated from a bounded")
    lines.append("> sample of an untrusted upload. Review it, and apply the DDL by hand only if")
    lines.append("> the data is trusted.")
    lines.append("")
    comp = f" ({p.detected_compression})" if p.detected_compression else ""
    lines.append(f"- **Source object:** `{p.source_key}` ({p.source_size:,} bytes)")
    lines.append(f"- **Detected format:** {p.detected_format}{comp}")
    lines.append(f"- **Rows sampled:** {p.sampled_rows:,}"
                 + (f" of ~{p.row_count_estimate:,}" if p.row_count_estimate else ""))
    lines.append(f"- **Confidence:** {p.confidence}")
    lines.append(f"- **Generated:** {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}")
    lines.append("")

    if p.error:
        lines.append("## ⛔ Could not inspect safely")
        lines.append("")
        lines.append("```")
        lines.append(p.error.strip())
        lines.append("```")
        lines.append("")
        lines.append("No schema was extracted. Treat this file as hostile until a human has "
                     "looked at it in an isolated environment.")
        return "\n".join(lines) + "\n"

    if p.columns:
        lines.append("## Proposed columns")
        lines.append("")
        lines.append("| column | type | alt types | null % | distinct~ | min | max | examples |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for c in p.columns:
            lines.append(
                f"| `{c.name}` | {c.trino_type} | {', '.join(c.alt_types) or '—'} | "
                f"{c.null_pct}% | {c.distinct_estimate if c.distinct_estimate is not None else '—'} | "
                f"{c.minimum or '—'} | {c.maximum or '—'} | "
                f"{'; '.join('`' + e + '`' for e in c.examples) or '—'} |"
            )
        lines.append("")

    if p.warnings:
        lines.append("## ⚠ Anomalies & notes")
        lines.append("")
        for w in p.warnings:
            lines.append(f"- {w}")
        lines.append("")

    ice, hive = _ddl(p)
    lines.append("## Proposed DDL — NOT APPLIED")
    lines.append("")
    lines.append("Managed Iceberg table (data copied in on `INSERT`):")
    lines.append("")
    lines.append("```sql")
    lines.append(ice)
    lines.append("```")
    lines.append("")
    lines.append("Or an external table over the raw files as they sit in MinIO:")
    lines.append("")
    lines.append("```sql")
    lines.append(hive)
    lines.append("```")
    lines.append("")

    if p.sample_rows and p.columns:
        lines.append("## Sample rows")
        lines.append("")
        head = "| " + " | ".join(f"`{c.name}`" for c in p.columns[:len(p.sample_rows[0])]) + " |"
        lines.append(head)
        lines.append("|" + "---|" * len(p.sample_rows[0]))
        for r in p.sample_rows:
            lines.append("| " + " | ".join(str(x).replace("|", "\\|") for x in r) + " |")
        lines.append("")

    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Processing pipeline
# --------------------------------------------------------------------------- #

_seen_lock = threading.Lock()


def _state_key(bucket: str, key: str, etag: str) -> str:
    h = hashlib.sha256(f"{bucket}/{key}:{etag}".encode()).hexdigest()[:16]
    return f"_state/{h}"


def _debounce_key(dataset_dir: str) -> str:
    hour = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H")
    h = hashlib.sha256(dataset_dir.encode()).hexdigest()[:12]
    return f"_state/dir-{h}-{hour}"


def already_done(s3, bucket: str, key: str, etag: str, dataset_dir: str) -> bool:
    for k in (_state_key(bucket, key, etag), _debounce_key(dataset_dir)):
        try:
            s3.head_object(Bucket=REPORTS_BUCKET, Key=k)
            return True
        except ClientError:
            continue
    return False


def mark_done(s3, bucket: str, key: str, etag: str, dataset_dir: str) -> None:
    for k in (_state_key(bucket, key, etag), _debounce_key(dataset_dir)):
        try:
            s3.put_object(Bucket=REPORTS_BUCKET, Key=k, Body=b"")
        except Exception as exc:  # noqa: BLE001
            log(f"could not write state {k}: {exc}")


def process_object(bucket: str, key: str, size: int, etag: str) -> None:
    s3 = _s3()
    dataset_dir, _ = _dataset_of(key)
    if key.endswith("/") or size == 0:
        return
    if key.split("/")[-1].startswith("."):
        return
    with _seen_lock:
        if already_done(s3, bucket, key, etag, dataset_dir):
            log(f"skip (already inspected): {key}")
            return
        mark_done(s3, bucket, key, etag, dataset_dir)

    log(f"inspecting s3://{bucket}/{key} ({size:,} bytes)")
    tmpdir = f"/tmp/inspect-{base64.urlsafe_b64encode(os.urandom(9)).decode()}"
    os.makedirs(tmpdir, exist_ok=True)
    path = os.path.join(tmpdir, "obj")
    error = None
    result: dict = {}
    try:
        head = _get_range(s3, bucket, key, 0, 65535)
        tail = _get_range(s3, bucket, key, max(0, size - 8), size - 1) if size > 8 else head
        fmt, compression = detect_format(key, head, tail)

        if fmt == "unsupported":
            result = _empty("unsupported", [
                "unrecognised or unsupported format (v1 handles Parquet, CSV/TSV, JSON/NDJSON)",
            ])
        elif fmt == "parquet":
            if size > MAX_DOWNLOAD_BYTES:
                result = _empty("parquet", [
                    f"file is {size / 1024 / 1024:.0f} MiB, over the "
                    f"{MAX_DOWNLOAD_BYTES // 1024 // 1024} MiB safe-download limit; not inspected. "
                    "Raise MAX_DOWNLOAD_BYTES or inspect it manually in isolation.",
                ])
            else:
                _download(s3, bucket, key, path, size)
                status, payload = run_sandboxed("parquet", path, True, None)
                if status == "ok":
                    result = payload
                else:
                    error = payload
        else:
            nbytes = min(size, MAX_TEXT_BYTES)
            data = _get_range(s3, bucket, key, 0, nbytes - 1)
            with open(path, "wb") as fh:
                fh.write(data)
            status, payload = run_sandboxed(fmt, path, True, compression)
            if status == "ok":
                result = payload
            else:
                error = payload
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
        log(f"error inspecting {key}: {error}")
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass

    proposal = build_proposal(key, size, result or _empty("unknown", []), error)
    _publish(s3, proposal)


def _get_range(s3, bucket: str, key: str, start: int, end: int) -> bytes:
    resp = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
    return resp["Body"].read()


def _download(s3, bucket: str, key: str, path: str, size: int) -> None:
    remaining = min(size, MAX_DOWNLOAD_BYTES)
    with open(path, "wb") as fh:
        offset = 0
        while offset < remaining:
            end = min(offset + 8 * 1024 * 1024, remaining) - 1
            fh.write(_get_range(s3, bucket, key, offset, end))
            offset = end + 1


def _publish(s3, p: Proposal) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"{p.dataset}/{ts}" if p.dataset else ts
    md = render_markdown(p)
    js = json.dumps(dataclasses.asdict(p), indent=2, default=str)
    for key, body, ctype in (
        (f"{base}.md", md, "text/markdown"),
        (f"{base}.json", js, "application/json"),
        (f"{p.dataset}/latest.md" if p.dataset else "latest.md", md, "text/markdown"),
    ):
        s3.put_object(Bucket=REPORTS_BUCKET, Key=key, Body=body.encode(), ContentType=ctype)
    log(f"proposal written: s3://{REPORTS_BUCKET}/{base}.md  (confidence={p.confidence}, "
        f"cols={len(p.columns)}, error={'yes' if p.error else 'no'})")


# --------------------------------------------------------------------------- #
# HTTP webhook listener
# --------------------------------------------------------------------------- #

_pool: concurrent.futures.ThreadPoolExecutor | None = None


def log(msg: str) -> None:
    print(f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _reply(self, code: int, body: bytes = b"") -> None:
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        self._reply(200 if self.path == "/healthz" else 404, b"ok" if self.path == "/healthz" else b"")

    def do_POST(self) -> None:  # noqa: N802
        if WEBHOOK_TOKEN:
            got = self.headers.get("Authorization", "")
            if got.startswith("Bearer "):
                got = got[7:]
            if got != WEBHOOK_TOKEN:
                self._reply(401, b"unauthorized")
                return
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        self._reply(200, b"accepted")  # ack fast; MinIO liveness pings land here too
        try:
            event = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return
        for rec in event.get("Records", []):
            if not rec.get("eventName", "").startswith("s3:ObjectCreated"):
                continue
            s3info = rec.get("s3", {})
            bucket = s3info.get("bucket", {}).get("name")
            obj = s3info.get("object", {})
            key = unquote_plus(obj.get("key", ""))
            if bucket != LANDING_BUCKET or not key:
                continue
            size = int(obj.get("size", 0) or 0)
            etag = obj.get("eTag", "")
            _pool.submit(_guard, bucket, key, size, etag)

    def log_message(self, *args: Any) -> None:  # quiet default access log
        return


def _guard(bucket: str, key: str, size: int, etag: str) -> None:
    try:
        process_object(bucket, key, size, etag)
    except Exception:  # noqa: BLE001
        log(f"unhandled error for {key}:\n{traceback.format_exc()}")


def main() -> None:
    global _pool
    _pool = concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS)
    log(f"schema-proposer listening on :{LISTEN_PORT}  "
        f"(landing={LANDING_BUCKET}, reports={REPORTS_BUCKET}, "
        f"max_download={MAX_DOWNLOAD_BYTES // 1024 // 1024} MiB)")
    ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
