default:

dist:
	mkdir -p dist
	cp etc/litestream.yml dist/litestream.yml
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e GOOS=linux -e GOARCH=amd64 golang:1.15 go build -v -o dist/litestream ./cmd/litestream
	tar -cz -f dist/litestream-linux-amd64.tar.gz -C dist litestream

deb: dist
ifndef LITESTREAM_VERSION
	$(error LITESTREAM_VERSION is undefined)
endif
	cat etc/nfpm.yml | envsubst > dist/nfpm.yml
	nfpm pkg --config dist/nfpm.yml --packager deb --target dist/litestream.deb 

clean:
	rm -rf dist

.PHONY: deb dist clean
