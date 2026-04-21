# Two-stage build: Alpine + Crystal gives us a fully-static binary
# (musl libc baked in + static-linked libgc / libpcre / libssl / libyaml),
# so the resulting image — and the extracted binary — runs on any Linux
# with a 2.6.32+ kernel: RHEL 8/9/10, bare metal, or scratch containers.
#
# MT is on (-Dpreview_mt); CRYSTAL_WORKERS=N at runtime picks the
# scheduler thread count.

# ---- build stage ----
FROM crystallang/crystal:1.20.0-alpine AS build

# Static-link prerequisites that aren't always pre-installed in the
# Alpine base. yaml-static and libxml2-static are the ones most likely
# to trip a --static build.
RUN apk add --no-cache \
    yaml-static \
    libxml2-static \
    xz-static \
    zlib-static \
    openssl-libs-static \
    zstd-static zstd-dev

WORKDIR /src

COPY shard.yml ./
RUN shards install --production || true

COPY src/ ./src/
COPY spec/ ./spec/

# Run specs as part of the build — fail fast in CI. Specs use the
# same MT flag so any MT-specific breakage shows here, not at runtime.
RUN crystal spec -Dpreview_mt

# Release build. --static produces a single fully-relocatable binary;
# --release enables LLVM optimizations; --no-debug strips DWARF.
RUN mkdir -p /out \
 && crystal build src/caravan_flow.cr \
    -o /out/caravan-flow \
    --release --no-debug --static -Dpreview_mt \
 && strip /out/caravan-flow \
 && ls -lh /out/caravan-flow

# ---- runtime stage ----
# Scratch = just the binary + config. No shell, no libc, no package
# manager. The static binary brings everything it needs.
FROM scratch

COPY --from=build /out/caravan-flow /caravan-flow
COPY config.yml /config.yml

EXPOSE 9092
ENV CRYSTAL_WORKERS=4

ENTRYPOINT ["/caravan-flow"]
CMD ["/config.yml"]
