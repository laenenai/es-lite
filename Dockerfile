# syntax=docker/dockerfile:1
#
# es-lited — the es-lite eventstore as a NATS service (ADR 0009).
# Multi-stage, static (CGO off; pgx + nats + natskit are pure Go), distroless
# runtime. The private module github.com/laenenai/natskit is fetched with a
# BuildKit secret so the token never lands in an image layer:
#
#   docker build --secret id=go_modules_token,env=GO_MODULES_TOKEN -t es-lited .
#
# (locally: GO_MODULES_TOKEN=$(gh auth token) docker build --secret ... .)

FROM golang:1.26-bookworm AS build
WORKDIR /src
ENV GOPRIVATE=github.com/laenenai/* \
    CGO_ENABLED=0 \
    GOOS=linux

# Build cmd/es-lited directly so only ITS dependency graph is fetched (pgx,
# nats, natskit — not modernc/sqlite, which the server never uses). The
# BuildKit secret gives git access to the private natskit module.
COPY . .
RUN --mount=type=secret,id=go_modules_token \
    git config --global url."https://x-access-token:$(cat /run/secrets/go_modules_token)@github.com/".insteadOf "https://github.com/" && \
    go build -trimpath -ldflags="-s -w" -o /out/es-lited ./cmd/es-lited

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/es-lited /es-lited
# NATS req/reply service (no inbound port); 8080 is the kubelet health probe.
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/es-lited"]
