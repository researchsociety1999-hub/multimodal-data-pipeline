import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from PIL import Image
from fixtures import create_fixture
from prepare_public import write_sample, download

class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_fixture_contracts(self):
        for case, total, valid in (("basic", 5, 3), ("edge", 7, 1)):
            expected = create_fixture(self.root / case, case)
            lines = (self.root / case / "metadata.jsonl").read_text().splitlines()
            self.assertEqual(len(lines), total)
            self.assertEqual(sum(row["should_accept"] for row in expected["labels"]), valid)
            self.assertEqual(sum(expected["rejections_by_reason"].values()), total - valid)
            self.assertEqual(len(set(row["record_hash"] for row in expected["labels"])), total)
            self.assertEqual([hashlib.sha256(line.encode()).hexdigest() for line in lines],
                             [row["record_hash"] for row in expected["labels"]])

    def test_local_public_adapter_preserves_bytes_and_provenance(self):
        data = io.BytesIO()
        Image.new("RGB", (8, 8), "red").save(data, "PNG")
        payload = data.getvalue()
        rows = [{"image": {"bytes": payload}, "caption_0": "A red square"}]
        output = self.root / "sample"
        report = write_sample(iter(rows), output,
            {"resolved_revision": "test-revision", "license_as_declared": "N/A"}, 1)
        self.assertEqual((output / "images" / "000000.img").read_bytes(), payload)
        self.assertEqual(report["selected_rows"][0]["image_sha256"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(report["sample_count"], 1)
        self.assertEqual(json.loads((output / "metadata.jsonl").read_text())["caption"], "A red square")
        with self.assertRaises(FileExistsError):
            write_sample(iter(rows), output, {}, 1)

    def test_failed_preparation_leaves_no_sample(self):
        output = self.root / "sample"
        with self.assertRaisesRegex(ValueError, "fewer source"):
            write_sample(iter([]), output, {}, 1)
        self.assertFalse(output.exists())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_network_download_requires_terms_acknowledgment(self):
        with self.assertRaisesRegex(ValueError, "acknowledge"):
            download(self.root / "sample", "main", 16, False)

if __name__ == "__main__":
    unittest.main()
