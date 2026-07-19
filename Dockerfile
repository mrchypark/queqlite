# syntax=docker/dockerfile:1

ARG RHIZA_PROFILE=all

FROM rust:1.95-trixie AS builder
ARG RHIZA_PROFILE
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        git \
        libclang-dev \
        libssl-dev \
        pkg-config \
        python3 \
    && rm -rf /var/lib/apt/lists/*
ARG LBUG_SOURCE_REPOSITORY=https://github.com/mrchypark/ladybug.git
ARG LBUG_SOURCE_REF=f95c0700b841fad79a842b819a7b1721b53569b3
RUN case "$RHIZA_PROFILE" in \
      graph|all) \
        git init /opt/ladybug \
        && git -C /opt/ladybug remote add origin "$LBUG_SOURCE_REPOSITORY" \
        && git -C /opt/ladybug fetch --depth=1 origin "$LBUG_SOURCE_REF" \
        && git -C /opt/ladybug checkout --detach FETCH_HEAD \
        && rm -rf /opt/ladybug/.git \
        ;; \
    esac
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=rhiza-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rhiza-cargo-target,target=/src/target,sharing=locked \
    if [ "$RHIZA_PROFILE" = graph ] || [ "$RHIZA_PROFILE" = all ]; then \
      export LBUG_SOURCE_DIR=/opt/ladybug; \
      export LBUG_RUST_BUILD_FROM_SOURCE=1; \
    fi; \
    case "$RHIZA_PROFILE" in \
      sql|graph|kv) \
        cargo build --release --locked -p rhiza-cli --bin rhiza \
          --no-default-features --features "$RHIZA_PROFILE,recorder-postcard-rpc" \
        ;; \
      all) \
        cargo build --release --locked -p rhiza-cli --bin rhiza --all-features \
        ;; \
      *) \
        echo "RHIZA_PROFILE must be sql|graph|kv|all" >&2; \
        exit 64 \
        ;; \
    esac \
    && install -D -m 0755 /src/target/release/rhiza /out/rhiza

FROM debian:trixie-slim
ARG RHIZA_PROFILE
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3t64 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/rhiza /usr/local/bin/rhiza
LABEL io.rhiza.build-profile="$RHIZA_PROFILE"
ENTRYPOINT ["rhiza"]
