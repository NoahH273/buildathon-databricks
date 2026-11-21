# Portable Rust build and Python runtime use the same Debian release.
FROM rust:1.85.0-bookworm AS rust-builder
WORKDIR /build
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
RUN --mount=type=cache,target=/build/target cargo build --release --locked && cp target/release/shopping-hybrid /build/shopping-hybrid

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=rust-builder /build/shopping-hybrid /app/bin/shopping-hybrid
COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod -R a+rX /app && chmod 755 /app/run.sh /app/bin/shopping-hybrid
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
CMD ["/app/run.sh"]
