#!/usr/bin/env python3
"""Package the verified musl executable; no network access or source rebuild."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import tarfile
import tomllib

root = Path(__file__).resolve().parent.parent
version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
revision = sys.argv[1]
if not re.fullmatch(r"[0-9a-f]{40}", revision):
    raise SystemExit("SOURCE_REVISION must be the full source commit")
if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-rc\.[0-9]+)?", version):
    raise SystemExit("release version must be X.Y.Z or X.Y.Z-rc.N")
binary = root / "target/x86_64-unknown-linux-musl/release/distill_fs"
manifest = {
    "component": "distill-fs",
    "version": version,
    "release_tag": f"v{version}",
    "source_revision": revision,
    "repository": "inclusionAI/distill-fs",
    "target": "x86_64-unknown-linux-musl",
    "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
}
files = {
    "distill_fs": binary.read_bytes(),
    "manifest.json": (json.dumps(manifest, indent=2) + "\n").encode(),
    "LICENSE": (root / "LICENSE").read_bytes(),
    "NOTICE": (root / "NOTICE").read_bytes(),
    "Cargo.lock": (root / "Cargo.lock").read_bytes(),
}
output = root / "dist"
output.mkdir(exist_ok=True)
archive = output / f"distill-fs-v{version}-linux-amd64.tar.gz"
# Stable metadata lets a single build be compared and promoted byte for byte.
with archive.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
    with tarfile.open(fileobj=zipped, mode="w") as tar:
        for name, data in files.items():
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            entry.mode = 0o755 if name == "distill_fs" else 0o644
            tar.addfile(entry, io.BytesIO(data))
checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
(output / "SHA256SUMS").write_text(f"{checksum}  {archive.name}\n")
print(f"{checksum}  {archive.name}")
