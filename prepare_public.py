"""Prepare a pinned Flickr8k sample; downloads only when explicitly invoked."""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import shutil
import tempfile

DATASET = "jxie/flickr8k"

def write_sample(rows, output, provenance, limit):
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(output)
    if not 1 <= limit <= 100:
        raise ValueError("limit must be 1..100")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".sample-", dir=output.parent))
    try:
        (stage / "images").mkdir()
        metadata, selected, total = [], [], 0
        for index, row in enumerate(itertools.islice(rows, limit)):
            payload = row["image"].get("bytes")
            caption = row.get("caption_0")
            if not isinstance(payload, (bytes, bytearray)) or not payload:
                raise ValueError("dataset must supply embedded image bytes")
            if not isinstance(caption, str) or not caption.strip():
                raise ValueError("missing caption_0")
            total += len(payload)
            if len(payload) > 20 * 1024 * 1024 or total > 128 * 1024 * 1024:
                raise ValueError("public sample exceeds byte budget")
            name = f"{index:06d}.img"
            (stage / "images" / name).write_bytes(payload)
            identifier = f"flickr8k-train-{index:06d}"
            metadata.append({"id": identifier, "path": name, "caption": caption,
                "source": f"{DATASET}@{provenance['resolved_revision']}:train:{index}",
                "license": provenance["license_as_declared"]})
            selected.append({"id": identifier, "row_index": index,
                             "image_sha256": hashlib.sha256(payload).hexdigest()})
        if len(metadata) != limit:
            raise ValueError("fewer source rows than requested")
        manifest = "".join(json.dumps(row, sort_keys=True) + "\n" for row in metadata)
        (stage / "metadata.jsonl").write_text(manifest)
        provenance = dict(provenance, sample_count=len(metadata), selection="first N train rows, source order",
            caption_field="caption_0", text_type="human dataset caption", selected_rows=selected,
            metadata_sha256=hashlib.sha256(manifest.encode()).hexdigest(), downloaded_image_bytes=total)
        (stage / "source.json").write_text(json.dumps(provenance, indent=2) + "\n")
        if output.exists():
            raise FileExistsError(output)
        os.rename(stage, output)
        return provenance
    finally:
        if stage.exists():
            shutil.rmtree(stage)

def download(output, revision, limit, acknowledge_terms):
    if not acknowledge_terms:
        raise ValueError("review upstream terms and pass --acknowledge-terms; public access does not grant redistribution rights")
    from datasets import Image, load_dataset
    from huggingface_hub import HfApi
    info = HfApi().dataset_info(DATASET, revision=revision)
    card = info.card_data or {}
    declared = card.get("license") or "N/A (not declared in dataset card)"
    if isinstance(declared, list):
        declared = ", ".join(declared)
    provenance = {"dataset": DATASET, "requested_revision": revision, "resolved_revision": info.sha,
        "split": "train", "license_as_declared": str(declared),
        "dataset_url": f"https://huggingface.co/datasets/{DATASET}",
        "terms_acknowledged": True, "redistribution_rights_verified": False}
    rows = load_dataset(DATASET, split="train", revision=info.sha, streaming=True)
    rows = rows.cast_column("image", Image(decode=False))
    return write_sample(rows, output, provenance, limit)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/public-flickr8k")
    parser.add_argument("--revision", default="main")
    parser.add_argument("--limit", type=int, default=16)
    parser.add_argument("--acknowledge-terms", action="store_true")
    args = parser.parse_args()
    print(json.dumps(download(args.output, args.revision, args.limit, args.acknowledge_terms), indent=2))
