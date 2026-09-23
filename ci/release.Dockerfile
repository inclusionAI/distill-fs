# Build on musl so the C dependencies and Rust use the same libc.
FROM rust:1.85.0-alpine3.21@sha256:bea885d2711087e67a9f7a7cd1a164976f4c35389478512af170730014d2452a AS build
ARG TARGETARCH
RUN apk add --no-cache build-base cmake git perl pkgconf python3 binutils && \
    rustup component add rustfmt
WORKDIR /work
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static" \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static" \
    TMPDIR=/work/test-tmp
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo/ .cargo/
COPY src/ src/
COPY tests/ tests/
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-musl ;; \
      arm64) target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    mkdir -p "$TMPDIR"; \
    cargo fmt --all -- --check; \
    rustup target add "$target"; \
    cargo build --locked --release --target "$target" --bin distill_fs
# Verify the final stripped binary before tests and packaging.
# Reject both dynamically linked executables and static PIEs with shared deps.
RUN case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-musl; machine='Advanced Micro Devices X86-64' ;; \
      arm64) target=aarch64-unknown-linux-musl; machine=AArch64 ;; \
    esac; \
    binary="target/$target/release/distill_fs"; \
    readelf -h "$binary" && \
    readelf -h "$binary" | grep -Eq "Machine:[[:space:]]+$machine$" && \
    ! readelf -l "$binary" | grep -q INTERP && \
    ! readelf -d "$binary" | grep -q NEEDED && \
    readelf --wide --sections "$binary" > /work/release-sections.txt && \
    ! grep -Eq '[[:space:]]\.(symtab|debug_[^[:space:]]*)[[:space:]]' /work/release-sections.txt

FROM build AS test
RUN case "$TARGETARCH" in amd64) target=x86_64-unknown-linux-musl ;; arm64) target=aarch64-unknown-linux-musl ;; esac; \
    cargo test --locked --release --target "$target"

FROM test AS package
ARG SOURCE_REVISION
ARG TARGETARCH
COPY ci/package-release.py ci/package-release.py
COPY LICENSE NOTICE ./
RUN case "$TARGETARCH" in amd64) target=x86_64-unknown-linux-musl ;; arm64) target=aarch64-unknown-linux-musl ;; esac; \
    python3 ci/package-release.py "$SOURCE_REVISION" "$TARGETARCH" && \
    mkdir /empty-root && \
    cp "target/$target/release/distill_fs" /empty-root/distill_fs && \
    chroot /empty-root /distill_fs --version && \
    chroot /empty-root /distill_fs mount --help

FROM scratch AS artifact
COPY --from=package /work/dist/ /
