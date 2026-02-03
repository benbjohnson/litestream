FROM golang:1.24 AS builder

WORKDIR /src/ltx
COPY . .

ARG LTX_VERSION=
ARG LTX_COMMIT=

RUN go build -ldflags "-s -w -X 'main.Version=${LTX_VERSION}' -X 'main.Commit=${LTX_COMMIT}' -extldflags '-static'" -o /usr/local/bin/ltx ./cmd/ltx


FROM scratch
COPY --from=builder /usr/local/bin/ltx /usr/local/bin/ltx
ENTRYPOINT ["/usr/local/bin/ltx"]
CMD []
