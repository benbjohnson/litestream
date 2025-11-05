default:

docker:
	docker build -t litestream .

.PHONY: vfs
vfs:
	mkdir -p dist
	go build -tags vfs,SQLITE3VFS_LOADABLE_EXT -o dist/litestream-vfs.a -buildmode=c-archive ./cmd/litestream-vfs
	mv dist/litestream-vfs.h src/litestream-vfs.h
	gcc -framework CoreFoundation -framework Security -lresolv -g -fPIC -shared -o dist/litestream-vfs.so src/litestream-vfs.c dist/litestream-vfs.a

vfs-test:
	go test -v -tags=vfs ./cmd/litestream-vfs

.PHONY: clean
clean:
	rm -rf dist

mcp-wrap:
	fly mcp wrap --mcp="./dist/litestream"  --bearer-token=$(FLY_MCP_BEARER_TOKEN) --debug -- mcp --debug

mcp-inspect:
	fly mcp proxy -i --url http://localhost:8080/ --bearer-token=$(FLY_MCP_BEARER_TOKEN)


.PHONY: default clean mcp-wrap mcp-inspect
