ARG GO_VERSION=1.27

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS builder

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

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/wsmon /wsmon

USER nonroot:nonroot

# server: WebSocket on 8080; metrics, /healthz and /readyz on 9090.
# client: metrics, /healthz and /readyz on 9090.
EXPOSE 8080 9090

# Usage: docker run IMAGE server|client|healthcheck|version
# Configuration: WSMON_* environment variables; list them with `server -h` / `client -h`.
ENTRYPOINT ["/wsmon"]
