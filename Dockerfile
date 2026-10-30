# syntax=docker/dockerfile:1
#
# es-lited — the es-lite eventstore as a NATS service (ADR 0009).
# Multi-stage, static (CGO off; pgx, nats and modernc/sqlite are pure Go),
# distroless runtime. There are no private module dependencies.

# --platform=$BUILDPLATFORM: run the builder natively and cross-compile to the
# target (pure Go, CGO off) — fast multi-arch without QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.27.3-bookworm AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
ENV CGO_ENABLED=0

# Build the four commands directly so only their dependency graphs are fetched.
COPY . .
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/es-lited ./cmd/es-lited && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/es-migrate ./cmd/es-migrate && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/es-relayd ./cmd/es-relayd && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/es-verify ./cmd/es-verify

FROM gcr.io/distroless/static-debian12:nonroot
# es-lited (service) + es-migrate (init container) + es-relayd (relay) +
# es-verify (chain audit / anchors, ADR 0029).
COPY --from=build /out/es-lited /es-lited
COPY --from=build /out/es-migrate /es-migrate
COPY --from=build /out/es-relayd /es-relayd
COPY --from=build /out/es-verify /es-verify
# NATS req/reply service (no inbound port); 8080 is the kubelet health probe.
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/es-lited"]
