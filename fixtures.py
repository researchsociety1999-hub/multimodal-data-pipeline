"""Deterministic, network-free basic and adversarial fixtures."""
import argparse
import hashlib
import json
from pathlib import Path
from PIL import Image

def create_fixture(root, case):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False)
    images = root / "images"
    images.mkdir()
    def image(name, color, fmt="PNG"):
        Image.new("RGB", (16, 16), color).save(images / name, format=fmt)
    def record(identifier, filename, caption="a synthetic square"):
        return {"id": identifier, "path": filename, "caption": caption,
                "source": "synthetic-fixture", "license": "CC0-1.0"}
    if case == "basic":
        for name, color in (("red.png", "red"), ("green.png", "green"), ("blue.png", "blue")):
            image(name, color)
        rows = [record("a", "red.png"), record("b", "green.png"), record("c", "blue.png"),
                record("missing", "absent.png"), record("z-duplicate", "red.png")]
        reasons = {"missing_image": 1, "duplicate_image": 1}
        accepted_ids = ["a", "b", "c"]
    elif case == "edge":
        image("clean.png", "red")
        image("other.png", "blue")
        image("unsupported.gif", "green", "GIF")
        (images / "broken.png").write_bytes(b"not an image")
        rows = [record("same-id", "clean.png", "a winner"), record("same-id", "clean.png", "z duplicate"),
                record("decode", "broken.png"), record("blank", "other.png", "  "),
                "{not valid json", record("escape", "../outside.png"), record("format", "unsupported.gif")]
        reasons = {name: 1 for name in ("decode_error", "missing_required_metadata", "malformed_json",
                                      "invalid_image_path", "unsupported_format", "duplicate_id")}
        accepted_ids = ["same-id"]
    else:
        raise ValueError("unknown case")
    lines = [json.dumps(row, sort_keys=True) if isinstance(row, dict) else row for row in rows]
    (root / "metadata.jsonl").write_text("\n".join(lines) + "\n")
    labels = [{"record_hash": hashlib.sha256(line.encode()).hexdigest(),
               "should_accept": index < 3 if case == "basic" else index == 0}
              for index, line in enumerate(lines)]
    expected = {"input_records": len(rows), "accepted_records": len(accepted_ids),
                "accepted_ids": accepted_ids, "rejections_by_reason": reasons, "labels": labels}
    (root / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")
    return expected

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--case", choices=["basic", "edge"], required=True)
    args = parser.parse_args()
    print(json.dumps(create_fixture(args.output, args.case), indent=2))
