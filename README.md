# Multimodal Data Pipeline: PySpark

Local Spark image-text ingestion: explicit-schema JSONL + binary images -> validation -> deterministic deduplication -> versioned Parquet -> read-back and quality reports. Legacy pipeline.py remains the separate PyArrow baseline.

## Evidence status

Synthetic fixture tests pass when run locally. No public-dataset run has happened yet. CI run logs and uploaded artifacts are the source of reproducible execution evidence.

## Setup

Use Python 3.11 and Java 17; CI installs Temurin 17. Top-level dependency versions are pinned, not a transitive lockfile.

```bash
python -m venv .venv
source .venv/bin/activate
# PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt -r requirements-spark.txt
java -version
EVIDENCE_DIR=artifacts/test-evidence python -m unittest -v test_pipeline test_prepare test_spark
```

## Synthetic reproduction

```bash
python fixtures.py --case basic --output data/basic
spark-submit --master 'local[2]' spark_pipeline.py --metadata data/basic/metadata.jsonl --images data/basic/images --output artifacts/spark --version basic-v1
python fixtures.py --case edge --output data/edge
spark-submit --master 'local[2]' spark_pipeline.py --metadata data/edge/metadata.jsonl --images data/edge/images --output artifacts/spark --version edge-v1
```

Expected, not measured public metrics:
- basic: 5 input, 3 accepted; missing_image: 1; duplicate_image: 1.
- edge: 7 input, 1 accepted; decode_error, missing_required_metadata, malformed_json, invalid_image_path, unsupported_format, duplicate_id: 1 each.

Fixtures write independent acceptance labels keyed by raw-line hash. Tests compare actual accepted/rejected records against labels, calculate defect precision/recall and confusion counts, and assert exact reasons, IDs, bytes, image hashes, dimensions, Parquet read-back, manifest checksums, no-overwrite, order invariance, failed-run cleanup, size/pixel limits, and flat-directory restrictions. These are controlled quality-gate evaluations, NOT semantic caption accuracy or production benchmarks.

Precision and recall of 1.0 on these tiny labelled fixtures confirm that the rules behave as designed on the fixtures; they are not a performance claim.

## Public image-caption sample

Source: https://huggingface.co/datasets/jxie/flickr8k

Confirmed in the dataset viewer: the image column is `image`; caption columns are `caption_0`, `caption_1`, `caption_2`, `caption_3`, and `caption_4`. The adapter uses `image` and `caption_0`; no adapter or test changes are required for column names.

Adapter: first 16 train rows, image plus caption_0; streaming without remote dataset code. It preserves image bytes and records resolved Hub revision SHA, row indices, image hashes, metadata checksum, and declared license in source.json. Acquisition can transfer more than N records over the network. First-N is reproducible at a pinned revision, not a representative random sample.

The retrieved card did not establish a license. Missing license is recorded as N/A, NOT invented. Review upstream terms before acknowledging; do not redistribute downloaded images, captions or Parquet without establishing rights. The local adapter test uses synthetic bytes; it does not prove Hub access.

```bash
python prepare_public.py --limit 16 --acknowledge-terms
spark-submit --master 'local[2]' spark_pipeline.py --metadata data/public-flickr8k/metadata.jsonl --images data/public-flickr8k/images --output artifacts/spark --version public-flickr8k-v1
cat artifacts/spark/public-flickr8k-v1/quality-report.json
```

For exact replay, pass --revision with source.json resolved_revision and use a fresh output directory. Acquisition fails on missing bytes/captions or insufficient rows; it never substitutes fixtures.

## Output and CI

Each named run contains accepted/ and rejected/ Parquet, quality-report.json and a checksum manifest.json. Reports record actual Spark/Python/Java versions, master, elapsed wall time, input checksum, counts, reasons and provenance. Never overwrite versions. A local staging-directory rename publishes the completed snapshot; failures clean staging.

CI runs legacy tests, preparation tests, six Spark tests, and the original demo on Python 3.11 / Java 17. Actions uploads measured JSON evidence. Public processing requires workflow_dispatch with public_sample and acknowledge_terms enabled; only public-run reports/manifests are uploaded, not its data. A green synthetic job does not prove the public run. Review report IDs/hashes before sharing.

The original-demo CI step copies only metadata-referenced image files into a clean temporary image directory and places the metadata beside it, so unrelated subfolders or hidden files in data/demo cannot fail image-directory preflight.

## Limits

Required strings: id, path, caption, source, license. Flat regular-file image directory; no nested directories, symlinks or hidden files. PNG/JPEG only, 20 MiB per image, 25 million pixels; 256 MiB input folder budget. Images are read before individual size rejection. Use trusted inputs: preflight is not race-proof against hostile concurrent changes.

One row per image hash intentionally discards alternative captions. Spark performs reading, joining, SQL quality rules, hashing, window deduplication, aggregation and Parquet writing. A worker-side Pillow UDF decodes images. Only grouped counts reach the driver during transformation; tiny tests collect fixture rows. The local driver reads output parts for checksums. Full bytes increase output size.

This is local[2], not a cluster benchmark. Named run versions are not content-derived identities. Publication assumes a local filesystem and one writer. No object-store transactions, PII detector, semantic model evaluation, training, or production deployment. Source revision is pinned per run, not hardcoded before first acquisition.

## Resume gate

Resume wording remains deferred until a real public-dataset run succeeds. Do not use synthetic fixture counts as public-dataset achievements.
