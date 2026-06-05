# Build stage
FROM golang:1.25-bookworm AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o main .

# Final stage
FROM debian:bookworm-slim

# Install extra dependencies for ZFS inspection and general debugging.
# zfsutils-linux provides zpool and zfs commands.
# procps provides ps and top.
# iproute2 provides ip command.
# curl is useful for general troubleshooting.
RUN apt-get update && apt-get install -y \
	zfsutils-linux \
	procps \
	iproute2 \
	curl \
	lsb-release \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main /app/main

# Ensure there is a data directory for the SQLite database
RUN mkdir -p /app/data

# Run the application
CMD ["/app/main"]
