# Shopping Sessions: native Rust pipeline (see rust-pipeline/).
FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY rust-pipeline/Cargo.toml rust-pipeline/Cargo.lock ./
COPY rust-pipeline/.cargo ./.cargo
COPY rust-pipeline/src ./src
# single-step build; the grep fails the build if a placeholder binary slipped through
RUN cargo build --release --locked \
 && cp target/release/shopping-sessions /shopping-sessions \
 && grep -q manifest.json /shopping-sessions

FROM debian:bookworm-slim
COPY --from=build /shopping-sessions /app/shopping-sessions
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000:1000
CMD ["/app/shopping-sessions"]
