# Two-stage build: Alpine + Crystal gives us a fully-static binary
# (musl libc baked in + static-linked libgc / libpcre / libssl / libyaml
# / libzstd), so the resulting image — and the extracted binary —
# runs on any Linux with a 2.6.32+ kernel.
#
# MT is on (-Dpreview_mt); CRYSTAL_WORKERS=N at runtime picks the
# scheduler thread count.
#
# Build context: expects the repo root so both caravan-flow-crystal/
# and crystal-avro/ are reachable (the `avro` shard is a path: dep).
# `scripts/build-static.sh` runs `docker build` from the right place.

# ---- build stage ----
FROM crystallang/crystal:1.20.0-alpine AS build

RUN apk add --no-cache \
    yaml-static \
    libxml2-static \
    xz-static \
    zlib-static \
    openssl-libs-static \
    zstd-static zstd-dev

WORKDIR /src

# Sibling shards have to be in place before `shards install` resolves
# the path: deps in caravan-flow-crystal/shard.yml.
COPY crystal-avro /src/crystal-avro
COPY crystal-jsonpath /src/crystal-jsonpath
COPY caravan-flow-crystal/shard.yml /src/caravan-flow-crystal/shard.yml
COPY caravan-flow-crystal/shard.lock /src/caravan-flow-crystal/shard.lock

WORKDIR /src/caravan-flow-crystal
# Drop --production: path-deps don't benefit from strict locking, and
# forcing it here collides with the way COPY stamps new mtimes inside
# the container. shards still resolves the same versions from shard.yml.
RUN shards install

COPY caravan-flow-crystal/src/ ./src/
COPY caravan-flow-crystal/spec/ ./spec/

# Specs run inside the container build — any regression breaks the
# build, not just the runtime. (Same flag we use at runtime.)
RUN crystal spec -Dpreview_mt

RUN mkdir -p /out \
 && crystal build src/caravan_flow.cr \
    -o /out/caravan-flow \
    --release --no-debug --static -Dpreview_mt \
 && strip /out/caravan-flow \
 && ls -lh /out/caravan-flow

# ---- runtime stage ----
FROM scratch

COPY --from=build /out/caravan-flow /caravan-flow
COPY caravan-flow-crystal/config.yml /config.yml

EXPOSE 9092
ENV CRYSTAL_WORKERS=4

ENTRYPOINT ["/caravan-flow"]
CMD ["/config.yml"]
