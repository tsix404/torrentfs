# Stage 1: Build
FROM archlinux:latest AS builder

RUN pacman -Syu --noconfirm && pacman -S --noconfirm \
    rust \
    fuse3 \
    libtorrent-rasterbar \
    boost \
    openssl \
    clang \
    pkgconf \
    gcc \
    && pacman -Scc --noconfirm

WORKDIR /app

# Copy manifests for dependency resolution caching
COPY Cargo.toml Cargo.lock ./
COPY libtorrent-sys/Cargo.toml libtorrent-sys/

# Copy all source and build
COPY src/ src/
COPY libtorrent-sys/ libtorrent-sys/

RUN cargo build --release

# Stage 2: Runtime
FROM archlinux:latest

RUN pacman -Syu --noconfirm && pacman -S --noconfirm \
    fuse3 \
    libtorrent-rasterbar \
    openssl \
    ca-certificates \
    && pacman -Scc --noconfirm

# Create non-root user for running the FUSE mount
RUN useradd -m -s /bin/bash torrentfs

COPY --from=builder /app/target/release/torrentfs /usr/local/bin/torrentfs

# Default mount point
RUN mkdir -p /mnt && chown torrentfs:torrentfs /mnt

USER torrentfs
WORKDIR /home/torrentfs

ENTRYPOINT ["/usr/local/bin/torrentfs"]
CMD ["/mnt"]