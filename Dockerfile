FROM golang:1.16 as builder
WORKDIR /src/litestream
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags '-w -extldflags "-static"' -tags sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

FROM alpine
COPY --from=builder /usr/local/bin/litestream /usr/local/bin/litestream
ENTRYPOINT ["/usr/local/bin/litestream"]
CMD []
