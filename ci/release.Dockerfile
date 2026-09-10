# Build on musl so the C dependencies and Rust use the same libc.
FROM rust:1.85.0-alpine3.21@sha256:bea885d2711087e67a9f7a7cd1a164976f4c35389478512af170730014d2452a AS build
RUN apk add --no-cache build-base cmake git perl pkgconf python3 binutils
WORKDIR /work
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static" \
    TMPDIR=/work/test-tmp
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo/ .cargo/
COPY src/ src/
COPY tests/ tests/
RUN mkdir -p "$TMPDIR" && \
    cargo build --locked --release --target x86_64-unknown-linux-musl --bin distill_fs
# Reject both dynamically linked executables and static PIEs with shared deps.
RUN binary=target/x86_64-unknown-linux-musl/release/distill_fs; \
    readelf -h "$binary" && \
    ! readelf -l "$binary" | grep -q INTERP && \
    ! readelf -d "$binary" | grep -q NEEDED

FROM build AS test
RUN cargo test --locked --release --target x86_64-unknown-linux-musl

FROM test AS package
ARG SOURCE_REVISION
COPY ci/package-release.py ci/package-release.py
COPY LICENSE NOTICE ./
RUN python3 ci/package-release.py "$SOURCE_REVISION" && \
    mkdir /empty-root && \
    cp target/x86_64-unknown-linux-musl/release/distill_fs /empty-root/distill_fs && \
    chroot /empty-root /distill_fs --version && \
    chroot /empty-root /distill_fs mount --help

FROM scratch AS artifact
COPY --from=package /work/dist/ /
