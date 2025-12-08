default:

docker:
	docker build -t litestream .

# VFS build configuration
VFS_BUILD_TAGS := vfs,SQLITE3VFS_LOADABLE_EXT
VFS_SRC := ./cmd/litestream-vfs
VFS_C_SRC := src/litestream-vfs.c
MACOSX_MIN_VERSION := 11.0
DARWIN_LDFLAGS := -framework CoreFoundation -framework Security -lresolv -mmacosx-version-min=$(MACOSX_MIN_VERSION)
LINUX_LDFLAGS := -lpthread -ldl -lm

.PHONY: vfs
vfs:
	mkdir -p dist
	go build -tags vfs,SQLITE3VFS_LOADABLE_EXT -o dist/litestream-vfs.a -buildmode=c-archive ./cmd/litestream-vfs
	mv dist/litestream-vfs.h src/litestream-vfs.h
	gcc -DSQLITE3VFS_LOADABLE_EXT -framework CoreFoundation -framework Security -lresolv -g -fPIC -shared -o dist/litestream-vfs.so src/litestream-vfs.c dist/litestream-vfs.a

.PHONY: vfs-linux-amd64
vfs-linux-amd64:
	mkdir -p dist
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
		go build -tags $(VFS_BUILD_TAGS) -o dist/litestream-vfs-linux-amd64.a -buildmode=c-archive $(VFS_SRC)
	cp dist/litestream-vfs-linux-amd64.h src/litestream-vfs.h
	gcc -DSQLITE3VFS_LOADABLE_EXT -g -fPIC -shared -o dist/litestream-vfs-linux-amd64.so \
		$(VFS_C_SRC) dist/litestream-vfs-linux-amd64.a $(LINUX_LDFLAGS)

.PHONY: vfs-linux-arm64
vfs-linux-arm64:
	mkdir -p dist
	CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc \
		go build -tags $(VFS_BUILD_TAGS) -o dist/litestream-vfs-linux-arm64.a -buildmode=c-archive $(VFS_SRC)
	cp dist/litestream-vfs-linux-arm64.h src/litestream-vfs.h
	aarch64-linux-gnu-gcc -DSQLITE3VFS_LOADABLE_EXT -g -fPIC -shared -o dist/litestream-vfs-linux-arm64.so \
		$(VFS_C_SRC) dist/litestream-vfs-linux-arm64.a $(LINUX_LDFLAGS)

.PHONY: vfs-darwin-amd64
vfs-darwin-amd64:
	mkdir -p dist
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 \
		go build -tags $(VFS_BUILD_TAGS) -o dist/litestream-vfs-darwin-amd64.a -buildmode=c-archive $(VFS_SRC)
	cp dist/litestream-vfs-darwin-amd64.h src/litestream-vfs.h
	clang -DSQLITE3VFS_LOADABLE_EXT -arch x86_64 -g -fPIC -shared -o dist/litestream-vfs-darwin-amd64.dylib \
		$(VFS_C_SRC) dist/litestream-vfs-darwin-amd64.a $(DARWIN_LDFLAGS)

.PHONY: vfs-darwin-arm64
vfs-darwin-arm64:
	mkdir -p dist
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 \
		go build -tags $(VFS_BUILD_TAGS) -o dist/litestream-vfs-darwin-arm64.a -buildmode=c-archive $(VFS_SRC)
	cp dist/litestream-vfs-darwin-arm64.h src/litestream-vfs.h
	clang -DSQLITE3VFS_LOADABLE_EXT -arch arm64 -g -fPIC -shared -o dist/litestream-vfs-darwin-arm64.dylib \
		$(VFS_C_SRC) dist/litestream-vfs-darwin-arm64.a $(DARWIN_LDFLAGS)

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
