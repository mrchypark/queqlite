FROM rust:1.95-trixie AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        libclang-dev \
        libssl-dev \
        pkg-config \
        python3 \
    && rm -rf /var/lib/apt/lists/*
ENV LBUG_BUILD_FROM_SOURCE=1
WORKDIR /src
COPY . .
RUN cargo build --release --locked --workspace --all-features --bin rhiza

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3t64 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/rhiza /usr/local/bin/rhiza
ENTRYPOINT ["rhiza"]
