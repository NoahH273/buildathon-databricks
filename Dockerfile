FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS builder
WORKDIR /build
COPY rust-pipeline/Cargo.toml rust-pipeline/Cargo.lock ./
COPY rust-pipeline/src ./src
# The submission binary cannot activate checkpointing or diagnostics at runtime.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=secret,id=cargo_ca,target=/etc/ssl/certs/ca-certificates.crt \
    cargo build --release --locked --no-default-features

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=builder /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8 MEM_GB=24
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
