# Shopping Sessions: native Rust pipeline (see rust-pipeline/).
FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY rust-pipeline/Cargo.toml rust-pipeline/Cargo.lock ./
COPY rust-pipeline/.cargo ./.cargo
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src target/release/deps/shopping_sessions*
COPY rust-pipeline/src ./src
RUN cargo build --release && cp target/release/shopping-sessions /shopping-sessions

FROM debian:bookworm-slim
COPY --from=build /shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
