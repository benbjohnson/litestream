default:

docker:
	docker build -t litestream .

dist-linux:
	mkdir -p dist
	cp etc/litestream.yml dist/litestream.yml
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e GOOS=linux -e GOARCH=amd64 golang:1.24 go build -v -ldflags "-s -w" -o dist/litestream ./cmd/litestream
	tar -cz -f dist/litestream-linux-amd64.tar.gz -C dist litestream

dist-linux-arm:
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e CGO_ENABLED=1 -e CC=arm-linux-gnueabihf-gcc -e GOOS=linux -e GOARCH=arm golang-xc:1.24 go build -v -o dist/litestream-linux-arm ./cmd/litestream

dist-linux-arm64:
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e CGO_ENABLED=1 -e CC=aarch64-linux-gnu-gcc -e GOOS=linux -e GOARCH=arm64 golang-xc:1.24 go build -v -o dist/litestream-linux-arm64 ./cmd/litestream

dist-macos:
ifndef LITESTREAM_VERSION
	$(error LITESTREAM_VERSION is undefined)
endif
	mkdir -p dist

	GOOS=darwin GOARCH=amd64 CC="gcc -target amd64-apple-macos11" CGO_ENABLED=1 go build -v -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}'"  -o dist/litestream ./cmd/litestream
	gon etc/gon.hcl
	mv dist/litestream.zip dist/litestream-${LITESTREAM_VERSION}-darwin-amd64.zip
	openssl dgst -sha256 dist/litestream-${LITESTREAM_VERSION}-darwin-amd64.zip

	GOOS=darwin GOARCH=arm64 CC="gcc -target arm64-apple-macos11" CGO_ENABLED=1 go build -v -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}'"  -o dist/litestream ./cmd/litestream
	gon etc/gon.hcl
	mv dist/litestream.zip dist/litestream-${LITESTREAM_VERSION}-darwin-arm64.zip
	openssl dgst -sha256 dist/litestream-${LITESTREAM_VERSION}-darwin-arm64.zip

clean:
	rm -rf dist

mcp-wrap:
	fly mcp wrap --mcp="./dist/litestream"  --bearer-token=$(FLY_MCP_BEARER_TOKEN) --debug -- mcp --debug

mcp-inspect:
	fly mcp proxy -i --url http://localhost:8080/ --bearer-token=$(FLY_MCP_BEARER_TOKEN)


.PHONY: default dist-linux dist-macos clean mcp-wrap mcp-inspect
