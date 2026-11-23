# Base images are pinned by digest; Renovate keeps tag and digest current.
# Keep the golang tag in step with GO_VERSION in the Makefile and go.mod.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-w -s -X main.version=${VERSION}" -o /out/wsmon ./cmd/wsmon

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=builder /out/wsmon /wsmon

USER nonroot:nonroot

# server: WebSocket on 8080; metrics, /healthz and /readyz on 9090.
# client: metrics, /healthz and /readyz on 9090.
EXPOSE 8080 9090

# Usage: docker run IMAGE server|client|healthcheck|version
# Configuration: WSMON_* environment variables; list them with `server -h` / `client -h`.
ENTRYPOINT ["/wsmon"]
