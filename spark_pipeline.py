"""Bounded local PySpark image-text ingestion with staged snapshot publication."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import tempfile
import time
from PIL import Image
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T

MAX_BYTES = 20 * 1024 * 1024
MAX_TOTAL = 256 * 1024 * 1024
MAX_PIXELS = 25_000_000
SCHEMA = T.StructType([T.StructField(k, T.StringType()) for k in
    ("id", "path", "caption", "source", "license", "_corrupt_record")])
IMAGE_SCHEMA = T.StructType([
    T.StructField("width", T.IntegerType()), T.StructField("height", T.IntegerType()),
    T.StructField("format", T.StringType()), T.StructField("error", T.StringType())])

@F.udf(IMAGE_SCHEMA)
def inspect_image(content):
    if content is None:
        return None, None, None, "missing_image"
    if len(content) > MAX_BYTES:
        return None, None, None, "oversized_image"
    try:
        data = bytes(content)
        with Image.open(io.BytesIO(data)) as image:
            width, height = image.size
            fmt = image.format
            if width * height > MAX_PIXELS:
                return width, height, fmt, "too_many_pixels"
            if fmt not in ("PNG", "JPEG"):
                return width, height, fmt, "unsupported_format"
            image.verify()
        with Image.open(io.BytesIO(data)) as image:
            image.load()
        return width, height, fmt, None
    except Exception:
        return None, None, None, "decode_error"

def preflight(image_dir):
    image_dir = Path(image_dir).resolve()
    if not image_dir.is_dir():
        raise ValueError("images must be a local directory")
    entries = list(image_dir.iterdir())
    if any(p.is_symlink() or not p.is_file() or p.name.startswith((".", "_")) for p in entries):
        raise ValueError("use a flat directory of regular, non-hidden image files")
    if sum(p.stat().st_size for p in entries) > MAX_TOTAL:
        raise ValueError("sample exceeds 256 MiB local input budget")
    return image_dir, entries

def build(spark, metadata, images, output, version):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version):
        raise ValueError("invalid version name")
    metadata = Path(metadata).resolve()
    images, entries = preflight(images)
    parent = Path(output).resolve()
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / version
    if target.exists():
        raise FileExistsError(target)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=parent))
    checked = None
    started = time.monotonic()
    try:
        raw = spark.read.text(str(metadata))
        parsed = raw.select(
            F.sha2("value", 256).alias("record_hash"),
            F.from_json("value", SCHEMA, {"mode": "PERMISSIVE"}).alias("record"))
        meta = parsed.select("record_hash", F.col("record").isNull().alias("parse_null"), "record.*")
        if entries:
            binary = spark.read.format("binaryFile").load(str(images)).select(
                F.regexp_extract("path", r"([^/]+)$", 1).alias("image_name"),
                F.col("length").alias("image_size"), F.col("content").alias("image_bytes"))
        else:
            binary = spark.createDataFrame([], "image_name string, image_size long, image_bytes binary")
        joined = meta.join(binary, meta.path == binary.image_name, "left").drop("image_name")
        missing = F.lit(False)
        for field in ("id", "path", "caption", "source", "license"):
            missing = missing | F.col(field).isNull() | (F.length(F.trim(F.col(field))) == 0)
        invalid_path = F.col("path").rlike(r"[/\\]") | F.col("path").isin(".", "..")
        checked = (joined.withColumn("image", inspect_image("image_bytes"))
            .withColumn("image_sha256", F.sha2("image_bytes", 256))
            .withColumn("reason", F.when(F.col("parse_null") | F.col("_corrupt_record").isNotNull(), "malformed_json")
                .when(missing, "missing_required_metadata").when(invalid_path, "invalid_image_path")
                .when(F.col("image_bytes").isNull(), "missing_image")
                .when(F.col("image_size") > MAX_BYTES, "oversized_image")
                .when(F.col("image.error").isNotNull(), F.col("image.error")) )
            .persist(StorageLevel.MEMORY_AND_DISK))
        total = checked.count()
        eligible = checked.filter("reason IS NULL")
        id_window = Window.partitionBy("id").orderBy("image_sha256", "path", "caption", "source", "license", "record_hash")
        ranked = eligible.withColumn("id_rank", F.row_number().over(id_window))
        image_window = Window.partitionBy("image_sha256").orderBy("id", "path", "caption", "source", "license", "record_hash")
        unique_ids = ranked.filter("id_rank = 1").withColumn("image_rank", F.row_number().over(image_window))
        accepted = unique_ids.filter("image_rank = 1").select(
            "record_hash", "id", "path", "caption", "source", "license", "image_sha256", "image_size",
            F.col("image.width").alias("width"), F.col("image.height").alias("height"),
            F.col("image.format").alias("format"), "image_bytes")
        reject_fields = ["record_hash", "id", "reason"]
        rejected = (checked.filter("reason IS NOT NULL").select(*reject_fields)
            .unionByName(ranked.filter("id_rank > 1").withColumn("reason", F.lit("duplicate_id")).select(*reject_fields))
            .unionByName(unique_ids.filter("image_rank > 1").withColumn("reason", F.lit("duplicate_image")).select(*reject_fields)))
        count = accepted.count()
        reasons = {r.reason: r['count'] for r in rejected.groupBy("reason").count().collect()}
        if total != count + sum(reasons.values()):
            raise RuntimeError("record accounting failed")
        if not count:
            raise ValueError("no accepted records; no snapshot published")
        accepted.write.mode("errorifexists").parquet(str(staging / "accepted"))
        rejected.write.mode("errorifexists").parquet(str(staging / "rejected"))
        saved = spark.read.parquet(str(staging / "accepted"))
        if saved.count() != count or saved.filter(F.col("image_bytes").isNull()).count():
            raise RuntimeError("Parquet read-back validation failed")
        source_path = metadata.parent / "source.json"
        source = json.loads(source_path.read_text()) if source_path.exists() else None
        report = {"schema_version": 1, "dataset_version": version, "input_records": total,
            "accepted_records": count, "rejected_records": sum(reasons.values()),
            "rejections_by_reason": reasons, "acceptance_rate": count / total,
            "parquet_readback_records": count, "elapsed_seconds": round(time.monotonic() - started, 3),
            "spark_version": spark.version, "python_version": platform.python_version(),
            "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
            "spark_master": spark.sparkContext.master,
            "metadata_sha256": hashlib.sha256(metadata.read_bytes()).hexdigest(),
            "source_provenance": source, "deduplication_policy": "one row per image hash"}
        (staging / "quality-report.json").write_text(json.dumps(report, indent=2) + "\n")
        checksums = {p.relative_to(staging).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(staging.rglob("*.parquet"))}
        (staging / "manifest.json").write_text(json.dumps({"version": version, "parquet_sha256": checksums}, indent=2) + "\n")
        if target.exists():
            raise FileExistsError(target)
        os.rename(staging, target)
        return report
    finally:
        if checked is not None:
            checked.unpersist()
        if staging.exists():
            shutil.rmtree(staging)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("metadata", "images", "output", "version"):
        parser.add_argument("--" + flag, required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("image-text-pipeline").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    try:
        print(json.dumps(build(spark, args.metadata, args.images, args.output, args.version), indent=2))
    finally:
        spark.stop()
