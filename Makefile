
GIT_UPDATE_INDEX  := $(shell git update-index --refresh > /dev/null 2>&1)
GIT_REVISION      ?= $(shell git rev-parse HEAD)
GIT_VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

GO                  ?= go
GO_BUILD_TAGS       ?= osusergo,netgo,sqlite_omit_load_extension
GO_BUILD_EXTLDFLAGS ?=
GO_BUILD_LDFLAGS    ?= "-s -w -X 'main.Version=$(GIT_VERSION)' -extldflags '$(GO_BUILD_EXTLDFLAGS)'"

DOCKER_IMAGE_NAME        ?= litestream/litestream
DOCKER_IMAGE_TAG         ?= $(shell echo $(GIT_VERSION) | tr '+' '_')
DOCKER_BUILD_LABELS       = --label org.opencontainers.image.title=litestream
DOCKER_BUILD_LABELS      += --label org.opencontainers.image.description="Fully-replicated database with no pain and little cost."
DOCKER_BUILD_LABELS      += --label org.opencontainers.image.url="https://litestream.io/"
DOCKER_BUILD_LABELS      += --label org.opencontainers.image.source="https://github.com/benbjohnson/litestream"
DOCKER_BUILD_LABELS      += --label org.opencontainers.image.revision=$(GIT_REVISION)
DOCKER_BUILD_LABELS      += --label org.opencontainers.image.version=$(GIT_VERSION)
DOCKER_BUILD_LABELS      += --label org.opencontainers.image.created=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
DOCKER_BUILD_ARGS         = --build-arg GIT_VERSION=$(GIT_VERSION)
DOCKER_BUILD_ARGS        += --build-arg GO_BUILD_EXTLDFLAGS="-static"
DOCKER_BUILD_PLATFORMS   ?= linux/amd64,linux/arm,linux/arm64

default:

build:
	CGO_ENABLED=1 $(GO) build -ldflags $(GO_BUILD_LDFLAGS) -tags $(GO_BUILD_TAGS) ./cmd/litestream

docker-build:
	docker buildx build $(DOCKER_BUILD_ARGS) \
		--platform=$(DOCKER_BUILD_PLATFORMS) \
		$(DOCKER_BUILD_LABELS) \
		-t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) \
		--load \
		.

docker-push:
	docker buildx build $(DOCKER_BUILD_ARGS) \
		--platform=$(DOCKER_BUILD_PLATFORMS) \
		$(DOCKER_BUILD_LABELS) \
		-t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) \
		--push \
		.

dist-linux:
	mkdir -p dist
	cp etc/litestream.yml dist/litestream.yml
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e GOOS=linux -e GOARCH=amd64 golang:1.16 go build -v -ldflags "-s -w" -o dist/litestream ./cmd/litestream
	tar -cz -f dist/litestream-linux-amd64.tar.gz -C dist litestream

dist-linux-arm:
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e CGO_ENABLED=1 -e CC=arm-linux-gnueabihf-gcc -e GOOS=linux -e GOARCH=arm golang-xc:1.16 go build -v -o dist/litestream-linux-arm ./cmd/litestream

dist-linux-arm64:
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e CGO_ENABLED=1 -e CC=aarch64-linux-gnu-gcc -e GOOS=linux -e GOARCH=arm64 golang-xc:1.16 go build -v -o dist/litestream-linux-arm64 ./cmd/litestream

dist-macos:
ifndef GIT_VERSION
	$(error GIT_VERSION is undefined)
endif
	mkdir -p dist

	GOOS=darwin GOARCH=amd64 CC="gcc -target amd64-apple-macos11" CGO_ENABLED=1 go build -v -ldflags "-s -w -X 'main.Version=$(GIT_VERSION)'"  -o dist/litestream ./cmd/litestream
	gon etc/gon.hcl
	mv dist/litestream.zip dist/litestream-$(GIT_VERSION)-darwin-amd64.zip
	openssl dgst -sha256 dist/litestream-$(GIT_VERSION)-darwin-amd64.zip

	GOOS=darwin GOARCH=arm64 CC="gcc -target arm64-apple-macos11" CGO_ENABLED=1 go build -v -ldflags "-s -w -X 'main.Version=$(GIT_VERSION)'"  -o dist/litestream ./cmd/litestream
	gon etc/gon.hcl
	mv dist/litestream.zip dist/litestream-$(GIT_VERSION)-darwin-arm64.zip
	openssl dgst -sha256 dist/litestream-$(GIT_VERSION)-darwin-arm64.zip

clean:
	rm -rf dist

.PHONY: default dist-linux dist-macos clean
