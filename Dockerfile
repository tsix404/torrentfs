# Stage 1: Build
FROM rust:1-bookworm AS builder

# Build dependencies: cmake + g++ for libtorrent, others for Rust FFI
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    g++ \
    libssl-dev \
    libfuse3-dev \
    libclang-dev \
    pkg-config \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Build Boost 1.86.0 from source — Debian Bookworm's Boost 1.74 lacks boost/json.hpp
# required by libtorrent 2.1.0. Install to /usr/local so cmake picks it up.
WORKDIR /tmp/boost-build
RUN curl -sSL https://archives.boost.io/release/1.86.0/source/boost_1_86_0.tar.gz -o boost.tar.gz && \
    tar xzf boost.tar.gz && \
    cd boost_1_86_0 && \
    ./bootstrap.sh --prefix=/usr/local --with-libraries=system,chrono,random,thread,json && \
    ./b2 install -j$(nproc) link=shared --layout=system && \
    ldconfig && \
    rm -rf /tmp/boost-build

# Build libtorrent 2.1.0 from source (shared libs)
WORKDIR /tmp/libtorrent-build
RUN git clone --depth 1 --branch v2.1.0 --recurse-submodules https://github.com/arvidn/libtorrent.git . && \
    cmake -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DBUILD_SHARED_LIBS=ON \
        -DCMAKE_CXX_STANDARD=17 \
        -DBOOST_ROOT=/usr/local && \
    cmake --build build -j$(nproc) && \
    cmake --install build && \
    rm -rf /tmp/libtorrent-build

RUN ldconfig

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

# Minimal runtime dependencies only (no libtorrent-rasterbar2.0 package)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfuse3-3 \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy libtorrent 2.1.0 and Boost 1.86 shared libs from builder
COPY --from=builder /usr/local/lib/libtorrent-rasterbar.so* /usr/local/lib/
COPY --from=builder /usr/local/lib/libboost_*.so* /usr/local/lib/
RUN ldconfig

# Create non-root user for running the FUSE mount
RUN useradd -m -s /bin/bash torrentfs

COPY --from=builder /app/target/release/torrentfs /usr/local/bin/torrentfs

# Default mount point
RUN mkdir -p /mnt && chown torrentfs:torrentfs /mnt

USER torrentfs
WORKDIR /home/torrentfs

ENTRYPOINT ["/usr/local/bin/torrentfs"]
CMD ["/mnt"]