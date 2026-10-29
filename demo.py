"""Generate synthetic images; no network or personal data."""
import argparse
import json
from pathlib import Path
from PIL import Image

def create_demo(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    records = []
    for index, color in enumerate(("red", "green", "blue")):
        name = color + ".png"
        Image.new("RGB", (32, 32), color).save(directory / name)
        records.append({"id": str(index), "path": name, "caption": color + " square",
                        "source": "synthetic-demo", "license": "CC0-1.0"})
    records += [dict(records[0], id="duplicate"),
                dict(records[0], id="missing", path="missing.png")]
    manifest = directory / "metadata.jsonl"
    manifest.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    return manifest

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/demo")
    print(create_demo(parser.parse_args().output))
