# MinIO test image built from upstream source.
#
# MinIO stopped publishing community images and binaries; minio/minio and
# minio/mc are archived and the upstream README directs community users to
# build from source. This image builds both from pinned upstream commits.
# Module downloads are verified against sum.golang.org.
#
# Integration tests and CI expect it tagged as:
#   litestream-minio:RELEASE.2025-09-07T16-13-09Z

FROM golang:1.26-bookworm AS build

# RELEASE.2025-09-07T16-13-09Z
ARG MINIO_COMMIT=01ce918d8279a20e4706b96a64396146894adee4
ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
ARG MINIO_VERSION=2025-09-07T16:13:09Z

# RELEASE.2025-08-13T08-35-41Z
ARG MC_COMMIT=7394ce0dd2a80935aded936b09fa12cbb3cb8096
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
ARG MC_VERSION=2025-08-13T08:35:41Z

ENV CGO_ENABLED=0 GOTOOLCHAIN=local

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go install -trimpath -ldflags "\
      -X github.com/minio/minio/cmd.Version=${MINIO_VERSION} \
      -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_RELEASE} \
      -X github.com/minio/minio/cmd.CommitID=${MINIO_COMMIT}" \
      github.com/minio/minio@${MINIO_COMMIT}

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go install -trimpath -ldflags "\
      -X github.com/minio/mc/cmd.Version=${MC_VERSION} \
      -X github.com/minio/mc/cmd.ReleaseTag=${MC_RELEASE} \
      -X github.com/minio/mc/cmd.CommitID=${MC_COMMIT}" \
      github.com/minio/mc@${MC_COMMIT}

FROM debian:bookworm-slim

COPY --from=build /go/bin/minio /go/bin/mc /usr/bin/

EXPOSE 9000 9001
ENTRYPOINT ["minio"]
