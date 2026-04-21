.PHONY: build build-release build-static run clean test image

# preview_mt: turns on Crystal's multithreaded runtime. Without it,
# all fibers run on a single OS thread — fine for I/O but not what we
# want for a NiFi-style fabric that needs parallel processor exec.
MT_FLAG := -Dpreview_mt

build:
	crystal build src/caravan_flow.cr -o bin/caravan-flow $(MT_FLAG)

build-release:
	crystal build src/caravan_flow.cr -o bin/caravan-flow --release --no-debug $(MT_FLAG)
	@ls -lh bin/caravan-flow

run: build
	CRYSTAL_WORKERS=4 ./bin/caravan-flow config.yml

test:
	crystal spec $(MT_FLAG)

# Fully static, portable-to-any-Linux binary via Alpine+Crystal container.
# Use this for distribution — no brew / glibc coupling to the host.
build-static:
	./scripts/build-static.sh

# Runtime image built from scratch (just the binary + config). Tiny.
image:
	docker build -t caravan-flow-crystal:latest .

clean:
	rm -rf bin lib .crystal .shards
