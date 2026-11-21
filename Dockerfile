# Shopping Sessions: native Rust pipeline (see rust-pipeline/).
FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY rust-pipeline/Cargo.toml rust-pipeline/Cargo.lock ./
COPY rust-pipeline/.cargo ./.cargo
COPY rust-pipeline/src ./src
# One plain build of the real sources (no dummy-main caching step, whose stale binary can be
# shipped when copied sources keep older mtimes). Fail the build if the binary is not the pipeline.
RUN cargo build --release --locked \
 && cp target/release/shopping-sessions /shopping-sessions \
 && grep -q manifest.json /shopping-sessions

FROM debian:bookworm-slim
COPY --from=build /shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
