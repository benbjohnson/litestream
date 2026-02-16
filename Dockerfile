FROM golang:1.24 AS builder

# Install build dependencies for VFS extension
RUN apt-get update && apt-get install -y gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /src/litestream
COPY . .

ARG LITESTREAM_VERSION=latest

# Build litestream binary
RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

# Build VFS loadable extension
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

# --- Hardened image (Scratch) ---
FROM alpine:3.21 AS certs
RUN apk --update add ca-certificates

FROM scratch AS hardened
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/local/bin/litestream /usr/local/bin/litestream
ENTRYPOINT ["/usr/local/bin/litestream"]
CMD []

# --- Default image (Debian) ---
FROM debian:bookworm-slim AS default

RUN apt-get update && \
	apt-get install -y ca-certificates sqlite3 && \
	rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/litestream /usr/local/bin/litestream
COPY --from=builder /src/litestream/dist/litestream-vfs.so /usr/local/lib/litestream-vfs.so

ENTRYPOINT ["/usr/local/bin/litestream"]
CMD []
