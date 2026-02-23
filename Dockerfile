# --- Build Stage ---
FROM vlang/vlang:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the entire project
COPY . .

# Build DataCore
# Note: We use -enable-globals as required by the performance modules.
# We also include use_openssl for secure connections.
RUN cd src && v -prod -enable-globals -d use_openssl -o /app/bin/datacore .

# --- Run Stage ---
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssl \
    ca-certificates \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    libpq5 \
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
