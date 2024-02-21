FROM --platform=${TARGETPLATFORM} golang:1.21.7-alpine3.19 as builder

WORKDIR /src/litestream

# Download dependencies first to cache them.
COPY go.mod go.sum ./
RUN go mod download

RUN apk add --no-cache make git gcc musl-dev

COPY . .

ARG GIT_VERSION=latest
ARG GO_BUILD_EXTLDFLAGS=-static

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	make build GIT_VERSION=${GIT_VERSION} GO_BUILD_EXTLDFLAGS=${GO_BUILD_EXTLDFLAGS}

# ------------------------------------------------------------------------------

FROM --platform=${TARGETPLATFORM} alpine:3.19.1

COPY --from=builder /src/litestream/litestream /usr/local/bin/litestream

ENTRYPOINT ["/usr/local/bin/litestream"]
CMD []
