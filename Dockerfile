# Stage 1: Build
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libfuse3-dev \
    libtorrent-rasterbar-dev \
    libssl-dev \
    libclang-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests for dependency resolution caching
COPY Cargo.toml Cargo.lock ./
COPY libtorrent-sys/Cargo.toml libtorrent-sys/

# Copy all source and build
COPY src/ src/
COPY libtorrent-sys/ libtorrent-sys/

RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libfuse3-3 \
    libtorrent-rasterbar2.0 \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for running the FUSE mount
RUN useradd -m -s /bin/bash torrentfs

COPY --from=builder /app/target/release/torrentfs /usr/local/bin/torrentfs

# Default mount point
RUN mkdir -p /mnt && chown torrentfs:torrentfs /mnt

USER torrentfs
WORKDIR /home/torrentfs

ENTRYPOINT ["/usr/local/bin/torrentfs"]
CMD ["/mnt"]