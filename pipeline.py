"""Local image + metadata -> validated, content-addressed Parquet snapshots."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
from PIL import Image

SCHEMA_VERSION = 1
MAX_BYTES = 20 * 1024 * 1024
MAX_PIXELS = 25_000_000

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def digest(data):
    return hashlib.sha256(data).hexdigest()

def validate(manifest):
    manifest = Path(manifest).resolve()
    base = manifest.parent
    rows, rejects, hashes, ids = [], [], set(), set()
    with manifest.open(encoding="utf-8") as source:
        for number, line in enumerate(source, 1):
            try:
                item = json.loads(line)
                if not isinstance(item, dict):
                    raise ValueError("record must be an object")
                for field in ("id", "path", "caption", "source", "license"):
                    if not isinstance(item.get(field), str) or not item[field].strip():
                        raise ValueError("missing or invalid field: " + field)
                if item["id"] in ids:
                    raise ValueError("duplicate id")
                path = (base / item["path"]).resolve()
                if not path.is_relative_to(base):
                    raise ValueError("image path escapes manifest directory")
                if not path.is_file() or not 0 < path.stat().st_size <= MAX_BYTES:
                    raise ValueError("missing, empty, or oversized image")
                payload = path.read_bytes()
                sha = digest(payload)
                if sha in hashes:
                    raise ValueError("duplicate image content")
                with Image.open(io.BytesIO(payload)) as image:
                    width, height = image.size
                    fmt = image.format
                    if width * height > MAX_PIXELS:
                        raise ValueError("image exceeds pixel limit")
                    if fmt not in ("PNG", "JPEG"):
                        raise ValueError("only PNG and JPEG supported")
                    image.verify()
                with Image.open(io.BytesIO(payload)) as image:
                    image.load()
                row = {k: item[k] for k in ("id", "caption", "source", "license")}
                row.update(image_path=path.relative_to(base).as_posix(), image_sha256=sha,
                           width=width, height=height, format=fmt, image_bytes=payload)
                rows.append(row)
                ids.add(item["id"])
                hashes.add(sha)
            except (ValueError, OSError, Image.DecompressionBombError) as error:
                # Do not echo input metadata or local absolute paths into reports.
                reason = str(error) if isinstance(error, ValueError) else type(error).__name__
                rejects.append({"line": number, "reason": reason})
    return sorted(rows, key=lambda row: row["id"]), rejects

def version_for(rows):
    metadata = [{k: v for k, v in row.items() if k != "image_bytes"} for row in rows]
    return digest(canonical({"schema_version": SCHEMA_VERSION, "rows": metadata}))

def build(manifest, output):
    import pyarrow as pa
    import pyarrow.parquet as pq
    rows, rejects = validate(manifest)
    if not rows:
        raise ValueError("no valid images; no dataset published")
    version = version_for(rows)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(name, pa.string()) for name in
        ("id", "caption", "source", "license", "image_path", "image_sha256")]
        + [("width", pa.int32()), ("height", pa.int32()), ("format", pa.string()),
           ("image_bytes", pa.binary())])
    target = output / version
    reused = target.exists()
    if reused:
        previous = json.loads((target / "manifest.json").read_text())
        if previous["dataset_version"] != version or digest((target / "data.parquet").read_bytes()) != previous["parquet_sha256"]:
            raise ValueError("existing snapshot integrity check failed")
    else:
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=output))
        try:
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), staging / "data.parquet", compression="zstd")
            info = {"schema_version": SCHEMA_VERSION, "dataset_version": version,
                    "accepted": len(rows), "parquet_sha256": digest((staging / "data.parquet").read_bytes()),
                    "pyarrow_version": pa.__version__}
            (staging / "manifest.json").write_bytes(canonical(info) + b"\n")
            os.rename(staging, target)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    report = {"dataset_version": version, "accepted": len(rows), "rejected": len(rejects),
              "snapshot_reused": reused, "rejects": rejects}
    report_id = digest(canonical(report))
    reports = output / "reports"
    reports.mkdir(exist_ok=True)
    (reports / (report_id + ".json")).write_bytes(canonical(report) + b"\n")
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", default="artifacts")
    args = parser.parse_args()
    print(json.dumps(build(args.manifest, args.output), indent=2))
