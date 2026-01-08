# =============================================================================
# BUILDER STAGES
# =============================================================================

FROM golang:1.24 AS builder-base
WORKDIR /src/litestream
COPY . .
ARG LITESTREAM_VERSION=latest

# Standard builder (static, no CGO)
FROM builder-base AS builder-standard
RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

# VFS builder (CGO enabled, Debian-based due to musl TLS issues)
# Alpine's musl libc has TLS (Thread Local Storage) compatibility issues with
# Go-built shared libraries. When loading the VFS extension on Alpine, you get
# TLS relocation errors. Debian's glibc works without these issues.
FROM builder-base AS builder-vfs
RUN apt-get update && apt-get install -y gcc libc6-dev && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	CGO_ENABLED=1 go build \
	-ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}'" \
	-tags "vfs,SQLITE3VFS_LOADABLE_EXT" \
	-o /usr/local/bin/litestream ./cmd/litestream
RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	mkdir -p dist && \
	CGO_ENABLED=1 go build \
	-tags "vfs,SQLITE3VFS_LOADABLE_EXT" \
	-buildmode=c-archive \
	-o dist/litestream-vfs.a ./cmd/litestream-vfs && \
	mv dist/litestream-vfs.h src/litestream-vfs.h && \
	gcc -DSQLITE3VFS_LOADABLE_EXT -g -fPIC -shared \
	-o dist/litestream-vfs.so \
	src/litestream-vfs.c \
	dist/litestream-vfs.a \
	-lpthread -ldl -lm

# =============================================================================
# RUNTIME STAGES
# =============================================================================

# Standard runtime (Alpine, minimal)
# Build with: docker build --target standard .
FROM alpine:3.20 AS standard
COPY --from=builder-standard /usr/local/bin/litestream /usr/local/bin/litestream
ENTRYPOINT ["/usr/local/bin/litestream"]
CMD []

# VFS runtime (Debian, with sqlite3 and VFS extension)
# Build with: docker build --target vfs .
FROM debian:bookworm-slim AS vfs
RUN apt-get update && \
	apt-get install -y ca-certificates sqlite3 && \
	rm -rf /var/lib/apt/lists/*
RUN mkdir -p /usr/local/lib
COPY --from=builder-vfs /usr/local/bin/litestream /usr/local/bin/litestream
COPY --from=builder-vfs /src/litestream/dist/litestream-vfs.so /usr/local/lib/litestream-vfs.so
ENV LITESTREAM_LOG_LEVEL=info
RUN printf '%s\n' \
	'#!/bin/sh' \
	'if [ -z "$LITESTREAM_REPLICA_URL" ]; then' \
	'    echo "Error: LITESTREAM_REPLICA_URL environment variable required"' \
	'    echo "Example: export LITESTREAM_REPLICA_URL='"'"'s3://bucket/path'"'"'"' \
	'    exit 1' \
	'fi' \
	'exec sqlite3 -cmd ".load /usr/local/lib/litestream-vfs.so sqlite3_litestreamvfs_init" "$@"' \
	> /usr/local/bin/litestream-vfs-sqlite && \
	chmod +x /usr/local/bin/litestream-vfs-sqlite
ENTRYPOINT ["/usr/local/bin/litestream"]
CMD []
