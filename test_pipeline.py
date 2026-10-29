import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from demo import create_demo
from pipeline import validate, version_for, build

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = create_demo(self.root / "input")

    def test_validation_and_duplicates(self):
        rows, rejects = validate(self.manifest)
        self.assertEqual((len(rows), len(rejects)), (3, 2))
        self.assertTrue(all(row["image_bytes"] and row["width"] == 32 for row in rows))

    def test_reorder_stable_and_metadata_changes_version(self):
        rows, _ = validate(self.manifest)
        original = version_for(rows)
        lines = self.manifest.read_text().splitlines()
        self.manifest.write_text("\n".join(reversed(lines[:3])))
        reordered, _ = validate(self.manifest)
        self.assertEqual(original, version_for(reordered))
        reordered[0]["caption"] = "changed"
        self.assertNotEqual(original, version_for(reordered))

    def test_traversal_and_bad_json_rejected(self):
        with self.manifest.open("a") as stream:
            stream.write(json.dumps({"id": "escape", "path": "../outside.png", "caption": "x", "source": "x", "license": "x"}) + "\n")
            stream.write("not json\n")
        _, rejects = validate(self.manifest)
        self.assertEqual(len(rejects), 4)
        self.assertIn("escapes", rejects[2]["reason"])

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow unavailable")
    def test_parquet_roundtrip_and_idempotence(self):
        import pyarrow.parquet as pq
        out = self.root / "artifacts"
        first = build(self.manifest, out)
        second = build(self.manifest, out)
        self.assertEqual(first["dataset_version"], second["dataset_version"])
        self.assertTrue(second["snapshot_reused"])
        table = pq.read_table(out / first["dataset_version"] / "data.parquet")
        self.assertEqual(table.num_rows, 3)
        self.assertIsInstance(table.to_pylist()[0]["image_bytes"], bytes)
        parquet = out / first["dataset_version"] / "data.parquet"
        parquet.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "integrity"):
            build(self.manifest, out)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow unavailable")
    def test_empty_fails_without_snapshot(self):
        self.manifest.write_text("")
        out = self.root / "empty-output"
        with self.assertRaisesRegex(ValueError, "no valid"):
            build(self.manifest, out)
        self.assertFalse(out.exists())

if __name__ == "__main__":
    unittest.main()
