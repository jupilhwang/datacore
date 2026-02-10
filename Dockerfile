# --- Build Stage ---
FROM debian:bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    libpq-dev \
    libnuma-dev \
    liburing-dev \
    zlib1g-dev \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install V from source (ensures we have a consistent Debian-based environment)
RUN git clone https://github.com/vlang/v /opt/vlang && \
    cd /opt/vlang && \
    make && \
    ln -s /opt/vlang/v /usr/local/bin/v

# Set working directory
WORKDIR /app

# Copy the entire project
COPY . .

# Build DataCore (Dynamic Build, no -prod for stability)
# Note: We use -enable-globals as required by the performance modules.
# We also include use_openssl for secure connections.
RUN mkdir -p /app/bin && cd src && v -enable-globals -d use_openssl \
    -cc gcc \
    -ldflags "-lssl -lcrypto -lsnappy -lstdc++ -llz4 -lzstd -lpq -lnuma -luring -lz -lpthread -lm -ldl" \
    -o /app/bin/datacore .

# --- Run Stage ---
# Using debian-slim for glibc compatibility with dynamic binary
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata wget \
    libssl3 libsnappy1v5 liblz4-1 libzstd1 libpq5 libnuma1 liburing2 zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/bin/datacore /usr/local/bin/datacore

# Copy default configuration
# Note: In production, users should mount their own config.toml
COPY config.toml /app/config.toml

# Expose ports
# 9092: Kafka Protocol
# 8080: REST API / WebSocket / Iceberg Catalog
# 8081: Schema Registry (if separate)
EXPOSE 9092 8080 8081

# Default command
# Using "broker start" as the default subcommand
ENTRYPOINT ["datacore"]
CMD ["broker", "start", "--config", "/app/config.toml"]
