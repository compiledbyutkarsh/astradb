FROM rust:latest AS builder

WORKDIR /app

COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/target/release/astradb .

COPY data ./data

EXPOSE 7878

ENV ASTRADB_HOST=0.0.0.0

CMD ["./astradb"]