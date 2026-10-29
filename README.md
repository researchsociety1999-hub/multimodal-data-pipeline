# Multimodal Data Pipeline

A small, local Python pipeline: PNG/JPEG images + JSONL text metadata -> validated, content-addressed Parquet snapshots. Image bytes and captions live together in Parquet; this is not just a CSV of image paths.

## Quick start

Python 3.11 is the CI target. No API keys or runtime network calls. Installation needs package access; GitHub Actions needs GitHub-hosted infrastructure.

```bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell instead: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m unittest -v
python demo.py
python pipeline.py --manifest data/demo/metadata.jsonl --output artifacts
```

Expected demo result: 3 accepted images, 2 rejected records (one duplicate and one missing file). This is an expected fixture result, not a performance benchmark. CI runs both the tests and the complete demo and uploads the generated artifacts.

## Input contract

One JSON object per line; all five fields are required nonempty strings:

```json
{"id":"sample-1","path":"images/example.png","caption":"A red square","source":"my-generated-fixtures","license":"CC0-1.0"}
```

Paths resolve relative to the metadata file directory. Files outside that directory, including symlink escapes, are rejected. Only use data you have rights and permission to process. The license field is recorded, not legally verified.

## Output and verification

```text
artifacts/
  <64-character dataset SHA-256>/
    data.parquet
    manifest.json
  reports/
    <report SHA-256>.json
```

The dataset hash covers canonical accepted metadata, image hashes, relative paths, and schema version, sorted by ID. It is a logical dataset identity, not a claim that different PyArrow releases produce identical Parquet bytes. The manifest stores the actual Parquet checksum and writer version. Re-runs reuse an existing snapshot after checking its checksum. Changed captions or image content create new versions. Rejection reports are separate from dataset identity. For conflicting duplicates, the first valid occurrence wins; reordering conflicting input can therefore change the dataset.

Inspect the data:

```bash
python -c "import glob, pyarrow.parquet as pq; p=glob.glob('artifacts/*/data.parquet')[0]; t=pq.read_table(p); print(t.schema); print('rows:',t.num_rows)"
```

Schema: strings `id`, `caption`, `source`, `license`, `image_path`, `image_sha256`, `format`; int32 `width`, `height`; binary `image_bytes`.

## Validation

Required metadata, unique IDs and image-content hashes, PNG/JPEG decoding, 20 MiB per-image limit, 25-million-pixel limit, and directory containment. Invalid rows are quarantined into reports; zero accepted records fails without publishing a snapshot. Snapshots are staged before rename. Use one writer per output directory.

## Test evidence and limits

During preparation, three validation/versioning/security tests passed locally. Two Parquet integration tests could not run because PyArrow was unavailable in the preparation environment. A successful full pipeline run is not yet claimed. GitHub Actions installs PyArrow and runs all five tests plus the demo; inspect the actual workflow result before claiming success.

This is a bounded, single-machine prototype: all accepted bytes are held in memory. It is not a distributed Spark/Ray pipeline, lakehouse, PII detector, benchmark, or production deployment. Captions can contain personal information; there is no automated redaction. No dependency lockfile is supplied yet. Do not ingest untrusted or sensitive datasets without additional controls. Demo images are generated locally; no real people or external datasets are included.
