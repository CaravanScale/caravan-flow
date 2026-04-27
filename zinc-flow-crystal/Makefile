.PHONY: build build-release build-static run clean test image

# preview_mt: turns on Crystal's multithreaded runtime. Without it,
# all fibers run on a single OS thread — fine for I/O but not what we
# want for a NiFi-style fabric that needs parallel processor exec.
MT_FLAG := -Dpreview_mt

# Extra link/runtime paths for libs installed via brew on the host
# (zstd, pcre). The Alpine container build doesn't need these — they
# come from apk packages and get static-linked.
BREW_LIB := /home/linuxbrew/.linuxbrew/lib
ZSTD_LIB := /home/linuxbrew/.linuxbrew/opt/zstd/lib
PCRE_LIB := /home/linuxbrew/.linuxbrew/opt/pcre/lib
HOST_LIBRARY_PATH := $(ZSTD_LIB):$(BREW_LIB):$(PCRE_LIB)

build:
	CRYSTAL_LIBRARY_PATH=$(HOST_LIBRARY_PATH) crystal build src/zinc_flow.cr -o bin/zinc-flow $(MT_FLAG)

build-release:
	CRYSTAL_LIBRARY_PATH=$(HOST_LIBRARY_PATH) crystal build src/zinc_flow.cr -o bin/zinc-flow --release --no-debug $(MT_FLAG)
	@ls -lh bin/zinc-flow

run: build
	LD_LIBRARY_PATH=$(HOST_LIBRARY_PATH) CRYSTAL_WORKERS=4 ./bin/zinc-flow config.yml

test:
	CRYSTAL_LIBRARY_PATH=$(HOST_LIBRARY_PATH) crystal spec $(MT_FLAG)

# Fully static, portable-to-any-Linux binary via Alpine+Crystal container.
# Use this for distribution — no brew / glibc coupling to the host.
build-static:
	./scripts/build-static.sh

# Runtime image built from scratch (just the binary + config). Tiny.
image:
	docker build -t zinc-flow-crystal:latest .

clean:
	rm -rf bin lib .crystal .shards
