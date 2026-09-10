# distill_fs

`distill_fs` is a read-only FUSE filesystem for raw disk images and Nydus
RAFS images. The current implementation combines:

- on-demand reads from local or remote storage
- sparse local cache files with bitmap sidecars
- optional chunk-level deduplication backed by LMDB
- an optional local/peer chunk server for chunk sharing
- optional OTLP metrics export

## What The Current Code Actually Supports

- `mount` is implemented on Linux only. On non-Linux targets, mounting returns
  an unsupported-operation error.
- `--src local` mounts a single raw file from the local filesystem.
- `--src oss` mounts a single raw file from a remote backend created from a
  Nydus `BackendConfigV2` JSON file. Despite the flag name, this path is not
  OSS-only. With the current build features it can use OSS, S3, registry, and
  HTTP-proxy backends exposed by `nydus-storage`.
- `--src nydus` mounts a Nydus RAFS v5 filesystem from a bootstrap plus backend
  config.
- Optional dedup uses:
  - `ChunkDB`: global content-addressed chunk storage
  - `IndexDB`: per-image offset-to-checksum metadata
- `serve-chunk` exposes a local `ChunkDB` over both TCP and a Unix socket, and
  can cooperate with peers plus an optional Redis-backed chunk ownership index.
- `gc-chunk` and `stats-chunk` operate on `ChunkDB`.

## Important Behavior And Constraints

- The filesystem is read-only.
- Raw-image mounts expose exactly one file at the mountpoint root. That file is
  named by `--name`.
- For raw-image mounts, `--name` must be a single path component. The current
  implementation reuses it as both the mounted filename and the raw-image
  backend identifier.
- For raw-image mounts, `--name` is also used as the dedup metadata identity.
  Reuse the same `--name` only for the same image namespace/content lineage when
  sharing an `image_meta_dir`.
- `--chunk-db-dir` and `--image-meta-dir` must be configured together or both
  omitted.
- The cache layer creates a sidecar bitmap file next to each cache file:
  `<cache path>.bitmap`.
- `--src local` opens the source file read-write. When dedup is enabled, chunks
  that have been persisted to `ChunkDB` may be hole-punched from that file. Use
  a dedicated writable cache copy if you need to preserve the original file
  byte-for-byte.
- In Nydus mode, `--name` is still required by the CLI parser, but it is not
  used by the mounted filesystem.
- In Nydus mode, `--cache-dir` is optional. If set, blob payloads are cached on
  disk and wrapped in the same cache/dedup stack. If omitted, blob ranges are
  fetched directly from the backend, while decompressed chunks can still be
  served from and stored into `ChunkDB` when dedup is enabled.
- The current Nydus read path supports RAFS v5. RAFS v6 images are rejected
  with an unsupported-operation error until the read path is moved to Nydus'
  `BlobDevice` flow.
- The current Nydus read path does not support encrypted chunks or batched
  chunks.
- `gc-chunk --dry-run` only skips deletion. It does not print a deletion plan.

## Build

Prerequisites:

- Rust 1.85.0; `rust-toolchain.toml` installs the matching formatter
- Linux with `/dev/fuse` access for `mount`
- writable directories for cache files, bitmap sidecars, LMDB data, and Unix
  sockets
- optional Redis if you want dynamic peer discovery or a Redis-backed chunk
  index

Build the main binary:

```bash
cargo build --bin distill_fs
```

Run the CLI help:

```bash
cargo run --bin distill_fs -- --help
```

## Static releases

GitHub releases provide `distill-fs-vX.Y.Z-linux-amd64.tar.gz` and `SHA256SUMS`. The archive contains the `distill_fs` executable, `manifest.json`, `LICENSE`, `NOTICE`, and the exact `Cargo.lock`. The executable uses musl and has no dynamic loader or shared-library dependencies. Mounting still requires Linux FUSE support and suitable privileges; remote HTTPS backends still need a CA certificate store on the host.

The first release is pending publication. Once published, download from `https://github.com/inclusionAI/distill-fs/releases/download/vX.Y.Z/`, verify the archive against a reviewed SHA-256 pin, extract it, and install `distill_fs` on `PATH`. Keep the license and provenance files with the binary. Production consumers must pin the release URL and checksum in source control; fetching a checksum alongside an unpinned binary is not an independent integrity check.

Build and test the same Linux/amd64 artifact locally with Docker:

```bash
docker build --platform linux/amd64 --target test -f ci/release.Dockerfile .
docker build --platform linux/amd64 -f ci/release.Dockerfile \
  --build-arg "SOURCE_REVISION=$(git rev-parse HEAD)" \
  --output type=local,dest=dist .
(cd dist && sha256sum -c SHA256SUMS)
```

Use a clean source checkout for release builds. `manifest.json` records the package version, source commit, musl target, and binary SHA-256. The Docker build checks the ELF for a dynamic interpreter and shared dependencies, then runs the executable in an empty chroot. The test target runs the default test suite against the musl release build; ignored FUSE tests need an explicitly privileged test environment. The pinned build image and `Cargo.lock` fix the toolchain and Rust dependency graph; Alpine build packages are resolved when the build runs, so independently rebuilding a tag is not a byte-for-byte reproducibility guarantee.

The `Static binary` workflow builds and tests pull requests, `main`, manual runs, and version tags, including real raw and RAFS v5 FUSE mount tests using the checksum-pinned Nydus v2.4.0 tool. Non-tag runs upload a reviewable Actions artifact without publishing a release. Pushing a feature branch alone does not trigger the workflow; opening or updating a pull request does. Manual dispatch becomes available once the workflow file exists on the default branch: use Actions → Static binary → Run workflow, or `gh workflow run release.yml --repo inclusionAI/distill-fs --ref main`. To publish, update the package version and lockfile, merge to `main`, and push a matching `vX.Y.Z` tag (or `vX.Y.Z-rc.N` for a prerelease). The workflow rejects version mismatches and tags outside `main`, then publishes the tested archive without rebuilding in the release job. Existing release assets are never overwritten. Review changes before pushing a release tag.

AKernel consumes this archive through its own `builder/distill-fs-versions.env` and `builder/scripts/install-distill-fs.sh`. Publish the distill-fs release first, verify its checksum, and update the AKernel pin. No sandboxd pipeline or gitlink change is required. The distill-fs submodule in AKernel is optional source reference and is no longer a build input.

## CLI Overview

The main binary exposes four subcommands:

| Command | Purpose |
| --- | --- |
| `mount` | Mount a raw image or Nydus filesystem |
| `serve-chunk` | Serve local chunks over TCP and Unix socket, optionally with peer coordination |
| `gc-chunk` | Garbage-collect old/LRU chunks from `ChunkDB` |
| `stats-chunk` | Print lightweight JSON stats for `ChunkDB` |

Global flags:

- `-d`, `--verbose`: enable debug logging

Common operational flags:

- `--daemon`: daemonize the process
- `--pid-file`: PID file path to use with `--daemon`
- `--log-file`: log file path; if set, logs go through the rotating file writer
- `--otel-endpoint`: enable OTLP metrics export

## Mount Modes

### `--src local`

Mount an existing local raw file as a single file in the mountpoint.

Required arguments:

- `--name`: filename exposed inside the mountpoint and dedup metadata key
- `--mountpoint`: FUSE mountpoint
- `--cache-file`: path to the existing local raw file

Example:

```bash
mkdir -p /mnt/raw

cargo run --bin distill_fs -- mount \
  --src local \
  --name disk.raw \
  --cache-file /data/disk.raw \
  --mountpoint /mnt/raw
```

### `--src oss`

Mount a remote raw file as a single file in the mountpoint. The implementation
uses `GeneralBackend`, which loads a Nydus `BackendConfigV2` JSON file. The
build enables local filesystem, OSS, S3, registry, and HTTP-proxy backends.

Required arguments:

- `--name`: remote object/blob identifier and mounted filename; it must still
  be valid as a single filename inside the mountpoint
- `--mountpoint`: FUSE mountpoint
- `--cfg`: backend config JSON
- `--cache-file`: writable local cache file path

Example:

```bash
mkdir -p /mnt/raw /var/cache/distill

cargo run --bin distill_fs -- mount \
  --src oss \
  --name rootfs.raw \
  --cfg /etc/distill/backend.json \
  --cache-file /var/cache/distill/rootfs.raw \
  --mountpoint /mnt/raw
```

### `--src nydus`

Mount a Nydus RAFS filesystem from a bootstrap file plus backend config.

Required arguments:

- `--name`: required by the CLI, but ignored by the Nydus mount logic
- `--mountpoint`: FUSE mountpoint
- `--bootstrap`: Nydus bootstrap path
- `--cfg`: backend config JSON

Optional but common:

- `--cache-dir`: writable directory for blob cache files

Example:

```bash
mkdir -p /mnt/nydus /var/cache/distill/blobs

cargo run --bin distill_fs -- mount \
  --src nydus \
  --name nydus-image \
  --bootstrap /images/bootstrap.rafs \
  --cfg /etc/distill/backend.json \
  --cache-dir /var/cache/distill/blobs \
  --mountpoint /mnt/nydus
```

## Enabling Dedup

Dedup is optional for both raw and Nydus mounts. To enable it, configure both:

- `--chunk-db-dir`
- `--image-meta-dir`

When enabled:

- `ChunkDB` stores chunk payloads globally by checksum
- `IndexDB` stores per-image range metadata
- raw-image reads can fall back from cache/backend to chunk reuse
- Nydus reads can serve decompressed chunks from `ChunkDB` and asynchronously
  persist newly read chunks into it

Example with dedup enabled:

```bash
mkdir -p /mnt/raw /var/cache/distill /var/lib/distill/chunkdb /var/lib/distill/imagedb

cargo run --bin distill_fs -- mount \
  --src oss \
  --name rootfs.raw \
  --cfg /etc/distill/backend.json \
  --cache-file /var/cache/distill/rootfs.raw \
  --chunk-db-dir /var/lib/distill/chunkdb \
  --image-meta-dir /var/lib/distill/imagedb \
  --mountpoint /mnt/raw
```

## Local And Peer Chunk Serving

`serve-chunk` serves a local `ChunkDB` in two ways at the same time:

- TCP on `0.0.0.0:<listen-port>`
- Unix socket for same-host clients

If `--chunk-server-sock` is omitted, the Unix socket defaults to:

```text
<chunk_db_dir>/chunkserver.sock
```

Mount processes that use the same `chunk_db_dir` automatically default to that
socket path too.

### Peer configuration

There are two peer-discovery modes:

- static peers via `--peer-addrs host:port,host:port`
- Redis discovery via `--peer-discovery redis://...`

If `--peer-discovery` is set, the current code expects it to be a Redis URL.
Other discovery schemes are rejected.

`--chunk-index-url` is separate from discovery. If configured, it enables a
Redis-backed chunk ownership index used by the peer client/chunk server.

Example:

```bash
mkdir -p /var/lib/distill/chunkdb

cargo run --bin distill_fs -- serve-chunk \
  --chunk-db-dir /var/lib/distill/chunkdb \
  --listen-port 9876 \
  --advertise-addr 10.0.0.11:9876 \
  --peer-discovery redis://10.0.0.20:6379/0 \
  --chunk-index-url redis://10.0.0.20:6379/1
```

Useful options:

- `--node-id`: explicit node identity; otherwise defaults to `$HOSTNAME` or
  `distill-fs-<pid>`
- `--chunk-index-ttl`: chunk ownership TTL in seconds, default `43200`
- `--tokio-worker-threads`: async runtime worker count, default `8`

## Garbage Collection

`gc-chunk` works on `ChunkDB` only.

Current GC behavior:

- delete chunks whose access time is older than 24 hours
- if LMDB usage is still above 90%, delete least-recently-used chunks in
  batches until usage falls to 80% or no more candidates exist

Run GC:

```bash
cargo run --bin distill_fs -- gc-chunk \
  --chunk-db-dir /var/lib/distill/chunkdb
```

Use a non-default local chunk server socket when needed:

```bash
cargo run --bin distill_fs -- gc-chunk \
  --chunk-db-dir /var/lib/distill/chunkdb \
  --chunk-server-sock /run/distill/custom-chunkserver.sock
```

Skip deletions:

```bash
cargo run --bin distill_fs -- gc-chunk \
  --chunk-db-dir /var/lib/distill/chunkdb \
  --dry-run
```

## ChunkDB Stats

`stats-chunk` prints lightweight JSON without a full chunk scan.

Example:

```bash
cargo run --bin distill_fs -- stats-chunk \
  --chunk-db-dir /var/lib/distill/chunkdb
```

Representative output:

```json
{
  "storage": {
    "total_size_bytes": 107374182400,
    "used_size_bytes": 52428800,
    "free_size_bytes": 107321753600,
    "usage_percent": "0.05"
  },
  "chunks": {
    "total_count": 1234
  },
  "access_time": {
    "oldest_epoch_secs": 1707500000,
    "newest_epoch_secs": 1707600000
  }
}
```

If the database has no access records yet, the access-time fields may be
`null`.

## Read Path Summary

### Raw images

1. Read through the FUSE file exposed at the mountpoint root.
2. If dedup metadata exists, try `ChunkDB` first for covered ranges.
3. If data is not available in `ChunkDB`, read from the cache/backend.
4. Queue background dedup work for touched 4 MiB chunks.
5. Once a chunk has been persisted to `ChunkDB`, the cache layer can invalidate
   that chunk and reclaim space by hole-punching the local cache file.

### Nydus images

1. Resolve inodes from the RAFS bootstrap.
2. For each file chunk, compute the expected checksum from RAFS metadata.
3. Try `ChunkDB` first.
4. On a miss, fetch the compressed chunk range from the backend or blob cache,
   then decompress if necessary.
5. Return the requested slice to FUSE and enqueue the full uncompressed chunk
   for background storage in `ChunkDB`.

The fixed chunk size used by the dedup/cache stack is 4 MiB.

## Backend Configuration

This project does not define its own remote-backend JSON schema. The `--cfg`
file for remote raw and Nydus modes is deserialized directly as Nydus
`BackendConfigV2`.

In practice this means:

- reuse the backend JSON format you already use with Nydus tooling
- the accepted fields depend on the backend type selected in that JSON
- the available backend types depend on which `nydus-storage` features were
  compiled into this binary

## Development

Common commands:

```bash
cargo fmt --all -- --check
cargo build --locked --bin distill_fs
cargo run --bin distill_fs -- --help
cargo test --locked
```

Redis integration tests are behind a feature flag:

```bash
DISTILL_FS_TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
  cargo test --locked --features redis-integration-tests \
  redis_ -- --test-threads=1
```

The database number must be explicit and non-zero. The tests run `FLUSHDB`
before and after each case, so the URL must point at a disposable test
database.

### Privileged mount tests

The mount tests are ignored during the default test run. They require Linux,
`/dev/fuse`, permission to mount and unmount FUSE filesystems, and either
`fusermount3`, `fusermount`, or `umount`.

Run the raw-image mount test:

```bash
cargo test --locked --test mount raw_mount_reads_file -- --ignored
```

The Nydus mount test also requires a `nydus-image` v2.4.0 binary. It builds a
minimal RAFS v5 image against the local filesystem backend before mounting it:

```bash
DISTILL_FS_NYDUS_IMAGE=/path/to/nydus-image \
  cargo test --locked --test mount nydus_mount_reads_file -- --ignored
```

These tests create all images, mountpoints, caches, and backend configuration
under temporary directories and clean them up after completion.

Additional benchmark binaries:

```bash
cargo run --bin chunkdb_bench -- --help
cargo run --bin peer_bench -- --help
```

## License

Apache-2.0
