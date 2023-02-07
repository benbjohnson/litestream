FROM golang:1.18-alpine as builder

# add ca certificates and timezone data files
# hadolint ignore=DL3018
RUN apk add -U --no-cache ca-certificates tzdata gcc musl-dev

# add unprivileged user
RUN adduser -s /bin/true -u 1000 -D -h /app app \
  && sed -i -r "/^(app|root)/!d" /etc/group /etc/passwd \
  && sed -i -r 's#^(.*):[^:]*$#\1:/sbin/nologin#' /etc/passwd

WORKDIR /src/litestream
COPY . .

ARG LITESTREAM_VERSION=latest

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

#
# ---
#

# start with empty image
FROM scratch

# add-in our timezone data file
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# add-in our ca certificates
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# add-in our unprivileged user
COPY --from=builder /etc/passwd /etc/group /etc/shadow /etc/

# from now on, run as the unprivileged user
USER app

# copy in our litestream binary
COPY --from=builder /usr/local/bin/litestream /litestream

ENTRYPOINT ["/litestream"]
CMD []
