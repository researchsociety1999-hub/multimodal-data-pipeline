"""Full Spark tests; fail rather than silently skip missing Spark/Java."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from PIL import Image
from pyspark.sql import SparkSession
from fixtures import create_fixture
from spark_pipeline import build, preflight

class SparkPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("pipeline-tests")
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def check_fixture(self, case):
        source = self.root / case
        expected = create_fixture(source, case)
        output = self.root / "output"
        report = build(self.spark, source / "metadata.jsonl", source / "images", output, case)
        self.assertEqual(report["input_records"], expected["input_records"])
        self.assertEqual(report["accepted_records"], expected["accepted_records"])
        self.assertEqual(report["rejections_by_reason"], expected["rejections_by_reason"])
        self.assertEqual(report["parquet_readback_records"], expected["accepted_records"])
        accepted = self.spark.read.parquet(str(output / case / "accepted")).collect()
        rejected = self.spark.read.parquet(str(output / case / "rejected")).collect()
        self.assertEqual(sorted(row.id for row in accepted), expected["accepted_ids"])
        for row in accepted:
            self.assertEqual(row.image_sha256, hashlib.sha256(bytes(row.image_bytes)).hexdigest())
            self.assertEqual((row.width, row.height), (16, 16))
        predicted_accepts = {row.record_hash for row in accepted}
        true_accepts = {row["record_hash"] for row in expected["labels"] if row["should_accept"]}
        all_records = {row["record_hash"] for row in expected["labels"]}
        self.assertEqual(len(all_records), expected["input_records"])
        expected_defects = all_records - true_accepts
        detected_defects = {row.record_hash for row in rejected}
        tp = len(expected_defects & detected_defects)
        fp = len(detected_defects - expected_defects)
        fn = len(expected_defects - detected_defects)
        self.assertEqual(predicted_accepts, true_accepts)
        self.assertEqual((fp, fn), (0, 0))
        evaluation = {"fixture": case, "true_positives": tp, "false_positives": fp,
                      "false_negatives": fn, "defect_precision": tp / (tp + fp),
                      "defect_recall": tp / (tp + fn), "quality_report": report}
        if os.environ.get("EVIDENCE_DIR"):
            evidence = Path(os.environ["EVIDENCE_DIR"])
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / (case + "-evaluation.json")).write_text(json.dumps(evaluation, indent=2) + "\n")
        manifest = json.loads((output / case / "manifest.json").read_text())
        for name, checksum in manifest["parquet_sha256"].items():
            self.assertEqual(hashlib.sha256((output / case / name).read_bytes()).hexdigest(), checksum)
        with self.assertRaises(FileExistsError):
            build(self.spark, source / "metadata.jsonl", source / "images", output, case)
        return source, output, accepted

    def test_basic_roundtrip_evaluation_and_no_overwrite(self):
        self.check_fixture("basic")

    def test_six_edge_reasons_and_evaluation(self):
        self.check_fixture("edge")

    def test_reordered_input_has_same_logical_output(self):
        source, output, first = self.check_fixture("basic")
        metadata = source / "metadata.jsonl"
        metadata.write_text("\n".join(reversed(metadata.read_text().splitlines())) + "\n")
        build(self.spark, metadata, source / "images", output, "reordered")
        second = self.spark.read.parquet(str(output / "reordered" / "accepted")).collect()
        self.assertEqual(sorted((r.id, r.image_sha256) for r in first), sorted((r.id, r.image_sha256) for r in second))

    def test_no_valid_records_publishes_nothing(self):
        source = self.root / "empty"
        source.mkdir()
        (source / "images").mkdir()
        (source / "metadata.jsonl").write_text("{bad json\n")
        output = self.root / "output"
        with self.assertRaisesRegex(ValueError, "no accepted"):
            build(self.spark, source / "metadata.jsonl", source / "images", output, "failed")
        self.assertFalse((output / "failed").exists())
        self.assertEqual(list(output.iterdir()), [])

    def test_oversized_and_pixel_limits(self):
        source = self.root / "limits"
        create_fixture(source, "basic")
        images = source / "images"
        with (images / "huge.bin").open("wb") as stream:
            stream.truncate(20 * 1024 * 1024 + 1)
        Image.new("1", (5001, 5001)).save(images / "pixels.png")
        with (source / "metadata.jsonl").open("a") as stream:
            for identifier, path in (("huge", "huge.bin"), ("pixels", "pixels.png")):
                stream.write(json.dumps({"id": identifier, "path": path, "caption": "test", "source": "synthetic", "license": "CC0-1.0"}) + "\n")
        report = build(self.spark, source / "metadata.jsonl", images, self.root / "out", "limits")
        self.assertEqual(report["rejections_by_reason"], {"missing_image": 1, "duplicate_image": 1, "oversized_image": 1, "too_many_pixels": 1})

    def test_preflight_rejects_nested_directories(self):
        images = self.root / "images"
        (images / "nested").mkdir(parents=True)
        with self.assertRaisesRegex(ValueError, "flat directory"):
            preflight(images)

if __name__ == "__main__":
    unittest.main()
