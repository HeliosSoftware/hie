# Linux image: the engine plus the Python runtime its transforms run on.
FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked --bin hie

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /hie
COPY --from=build /src/target/release/hie /usr/local/bin/hie
COPY hie_runtime/ /hie/hie_runtime/
COPY transforms/ /hie/transforms/
COPY sources/ /hie/sources/
COPY hie.yaml /hie/hie.yaml
ENV HIE_CONFIG=/hie/hie.yaml \
    HIE_RUNTIME_DIR=/hie/hie_runtime \
    HIE_LOG=info
EXPOSE 8090
ENTRYPOINT ["hie"]
