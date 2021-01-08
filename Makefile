default:

dist:
	mkdir -p dist
	docker run --rm -v "${PWD}":/usr/src/litestream -w /usr/src/litestream -e GOOS=linux -e GOARCH=amd64 golang:1.15 go build -v -o dist/litestream .
	tar -cz -f dist/litestream-linux-amd64.tar.gz -C dist litestream
	rm dist/litestream

.PHONY: dist