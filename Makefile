default:

dist-linux:
	mkdir -p dist
	cp etc/litestream.yml dist/litestream.yml
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e GOOS=linux -e GOARCH=amd64 golang:1.15 go build -v -o dist/litestream ./cmd/litestream
	tar -cz -f dist/litestream-linux-amd64.tar.gz -C dist litestream

dist-macos:
ifndef LITESTREAM_VERSION
	$(error LITESTREAM_VERSION is undefined)
endif
	mkdir -p dist
	go build -v -ldflags "-X 'main.Version=${LITESTREAM_VERSION}'"  -o dist/litestream ./cmd/litestream
	gon etc/gon.hcl
	mv dist/litestream.zip dist/litestream-${LITESTREAM_VERSION}-darwin-amd64.zip
	openssl dgst -sha256 dist/litestream-${LITESTREAM_VERSION}-darwin-amd64.zip

clean:
	rm -rf dist

.PHONY: default dist-linux dist-macos clean
