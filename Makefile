IMAGE?=wsmon
VERSION?=1.0.1
PLATFORMS=linux/amd64,linux/arm64
BINARY=wsmon
GO_VERSION=1.27

# All Go commands run inside a build container; no local Go toolchain is needed.
# -buildvcs=false: the container runs as root, and on Linux hosts (CI) the
# bind-mounted checkout belongs to another uid, so git refuses it ("dubious
# ownership") and every `go build`, including the ones tests run, fails with
# "error obtaining VCS status". The version comes from -ldflags instead.
GO_IMAGE=golang:$(GO_VERSION)
GO_DOCKER=docker run --rm -e GOFLAGS=-buildvcs=false $(GO_DOCKER_ENV) \
	-v $(CURDIR):/work -w /work \
	-v wsmon-gomod:/go/pkg/mod -v wsmon-gobuild:/root/.cache/go-build \
	$(GO_IMAGE)
LDFLAGS=-w -s -X main.version=$(VERSION)
# The Dockerfile pins its own base images; GO_VERSION here is only the test/build
# container. Keep the two in step (Renovate groups them).
BUILD_ARGS=--build-arg VERSION=$(VERSION)
HOST_OS:=$(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH:=$(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

# Server image for test-contract-image and test-fault. Defaults to the local
# build; set SERVER_IMAGE (and SERVER_ARGS) to test another image.
LOCAL_IMAGE=wsmon:local
SERVER_IMAGE?=$(LOCAL_IMAGE)
SERVER_ARGS?=server
CONTRACT_TOKEN?=contract-test-token

.PHONY: image-local image setup-buildx \
	test test-acceptance test-contract test-contract-image test-fault test-soak lint fmt tidy \
	helm-test helm-package helm-push check-version \
	build-local build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-all-archs \
	run-server run-client clean info

# ---------------------------------------------------------------------------
# Images.

# Single-architecture image for the host, tagged wsmon:local.
image-local:
	docker build $(BUILD_ARGS) -t $(LOCAL_IMAGE) .

# Image tagged $(IMAGE):$(VERSION).
#   make image                  host architecture only, loaded into the local
#                               Docker image store (shows up in `docker images`)
#   make image PUSH=1           all $(PLATFORMS), pushed to IMAGE's registry, e.g.
#                               make image IMAGE=ghcr.io/<owner>/wsmon PUSH=1
#   make image ARCHIVE=1        all $(PLATFORMS), written to $(IMAGE_ARCHIVE)
# IMAGE_BUILD_FLAGS is passed through to buildx, e.g. --metadata-file, --sbom,
# --provenance, --label, --annotation (see .github/workflows/release.yaml).
BUILDER=wsmon-builder
# BuildKit image for the builder; empty means buildx's default. CI pins it by
# digest (see .github/workflows), since it builds the published image.
BUILDKIT_IMAGE?=
IMAGE_ARCHIVE=$(BINARY)-$(VERSION)-oci.tar
IMAGE_OUTPUT=$(if $(PUSH),--platform $(PLATFORMS) --push,$(if $(ARCHIVE),--platform $(PLATFORMS) --output type=oci$(,)dest=$(IMAGE_ARCHIVE),--platform linux/$(HOST_ARCH) --load))
,:=,
setup-buildx:
	docker buildx inspect $(BUILDER) >/dev/null 2>&1 || \
		docker buildx create --platform=$(PLATFORMS) --name $(BUILDER) --driver=docker-container \
			$(if $(BUILDKIT_IMAGE),--driver-opt image=$(BUILDKIT_IMAGE))
	docker buildx inspect --bootstrap $(BUILDER)

image: setup-buildx
	docker buildx build --builder $(BUILDER) $(IMAGE_OUTPUT) $(BUILD_ARGS) $(IMAGE_BUILD_FLAGS) -t $(IMAGE):$(VERSION) .

# ---------------------------------------------------------------------------
# Helm chart (needs a local helm).

CHART=helm/wsmon
CHART_DIST=dist
# helm push appends the chart name: oci://ghcr.io/<owner>/wsmon-helm/wsmon.
CHART_REPO?=oci://ghcr.io/asappinc-opensource/wsmon-helm

# helm lint, then every ci/ fixture: pass-*.yaml must render, fail-*.yaml must
# fail with an error containing its "# expect:" line.
helm-test:
	helm lint $(CHART)
	@rc=0; for f in $(CHART)/ci/*.yaml; do \
		name=$$(basename $$f); out=$$(helm template wsmon $(CHART) -f $$f 2>&1); status=$$?; \
		case $$name in \
		pass-*) if [ $$status -ne 0 ]; then echo "FAIL $$name: did not render"; echo "$$out" | tail -3; rc=1; else echo "ok   $$name"; fi ;; \
		fail-*) expect=$$(sed -n 's/^# expect: //p' $$f | head -1); \
			if [ -z "$$expect" ]; then echo "FAIL $$name: no '# expect:' line"; rc=1; \
			elif [ $$status -eq 0 ]; then echo "FAIL $$name: rendered, expected: $$expect"; rc=1; \
			elif ! printf '%s' "$$out" | grep -qF -- "$$expect"; then echo "FAIL $$name: expected: $$expect"; echo "$$out" | tail -3; rc=1; \
			else echo "ok   $$name"; fi ;; \
		*) echo "FAIL $$name: fixtures must be named pass-* or fail-*"; rc=1 ;; \
		esac; \
	done; exit $$rc

helm-package:
	helm package $(CHART) -d $(CHART_DIST)

# Prints helm's "Digest: sha256:..." line; release.yaml signs the last one.
# Retried: GHCR can answer 404 when helm tags the manifest it has just
# uploaded ('failed to perform "Tag" on destination ... not found'). The
# same .tgz always has the same digest, so a retry is idempotent.
HELM_PUSH_ATTEMPTS?=4
helm-push: helm-package
	@n=1; until helm push $(CHART_DIST)/$(BINARY)-$(VERSION).tgz $(CHART_REPO); do \
		[ $$n -ge $(HELM_PUSH_ATTEMPTS) ] && { echo "helm push failed after $$n attempts"; exit 1; }; \
		echo "helm push attempt $$n failed; retrying in $$((n * 5))s"; sleep $$((n * 5)); n=$$((n + 1)); \
	done

# One version everywhere: Makefile VERSION == Chart version == Chart appVersion,
# and == the git tag without its "v" when TAG is set (release.yaml).
# TAG is read from the environment by the shell ($$TAG), never pasted into the
# recipe by make: a git tag name may contain shell syntax.
check-version:
	@chart=$$(awk '/^version:/{gsub(/"/,"",$$2); print $$2}' $(CHART)/Chart.yaml); \
	app=$$(awk '/^appVersion:/{gsub(/"/,"",$$2); print $$2}' $(CHART)/Chart.yaml); \
	rc=0; \
	[ "$$chart" = "$(VERSION)" ] || { echo "Chart.yaml version $$chart != VERSION $(VERSION)"; rc=1; }; \
	[ "$$app" = "$(VERSION)" ] || { echo "Chart.yaml appVersion $$app != VERSION $(VERSION)"; rc=1; }; \
	if [ -n "$${TAG:-}" ] && [ "$$TAG" != "v$(VERSION)" ]; then echo "tag $$TAG != v$(VERSION)"; rc=1; fi; \
	[ $$rc -eq 0 ] && echo "version $(VERSION) consistent"; exit $$rc

# ---------------------------------------------------------------------------
# Tests (all in the Go build container).

# Fast suite: shortened durations, race detector on.
test:
	$(GO_DOCKER) sh -c 'go test -race -short -count=1 ./... && \
		go test -race -short -count=1 -tags wsmontest ./test/client/contract/'

# Full acceptance suite with full test durations.
test-acceptance:
	$(GO_DOCKER) sh -c 'go test -race -count=1 -timeout 30m ./... && \
		go test -race -count=1 -timeout 30m -tags wsmontest ./test/client/contract/'

# Client/server contract suite against `wsmon server` built from this tree.
# SHORT=1 passes -short.
test-contract:
	$(GO_DOCKER) go test -tags wsmontest -count=1 -v -timeout 30m $(if $(SHORT),-short,) ./test/client/contract/

# Contract suite against a server image (SERVER_IMAGE) on a private Docker
# network. SHORT=1 passes -short.
CONTRACT_SERVER=wsmon-contract-server
CONTRACT_NET=wsmon-contract
test-contract-image: GO_DOCKER_ENV=--network $(CONTRACT_NET) --name wsmon-contract-runner \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-e WSMON_CONTRACT_URL=ws://$(CONTRACT_SERVER):8080/__synthetic/ws \
	-e WSMON_CONTRACT_METRICS_URL=http://$(CONTRACT_SERVER):9090/metrics \
	-e WSMON_CONTRACT_TOKEN=$(CONTRACT_TOKEN) \
	-e WSMON_CONTRACT_SERVER_CONTAINER=$(CONTRACT_SERVER)
test-contract-image: $(if $(filter $(LOCAL_IMAGE),$(SERVER_IMAGE)),image-local,)
	@docker rm -f $(CONTRACT_SERVER) >/dev/null 2>&1; docker network rm $(CONTRACT_NET) >/dev/null 2>&1; \
	trap 'rc=$$?; [ $$rc -eq 0 ] || { echo "--- $(CONTRACT_SERVER) logs (tail) ---"; docker logs --tail 50 $(CONTRACT_SERVER) 2>&1; }; \
		docker rm -f $(CONTRACT_SERVER) >/dev/null 2>&1; docker network rm $(CONTRACT_NET) >/dev/null 2>&1; exit $$rc' EXIT; \
	docker network create $(CONTRACT_NET) >/dev/null || exit 1; \
	docker run -d --name $(CONTRACT_SERVER) --network $(CONTRACT_NET) \
		-e WSMON_AUTH_TOKEN=$(CONTRACT_TOKEN) \
		-e WSMON_DRAIN_DELAY=1s \
		-e WSMON_SHUTDOWN_TIMEOUT=5s \
		-e WSMON_NAME=contract-server \
		-e WSMON_LABELS=zone=contract-az \
		-e WSMON_ENABLE_PERMESSAGE_DEFLATE=true \
		-e WSMON_OTLP_ENDPOINT=wsmon-contract-runner:4317 \
		-e WSMON_TRACE_SAMPLE_RATIO=1 \
		$(SERVER_IMAGE) $(SERVER_ARGS) >/dev/null || exit 1; \
	echo "server image: $(SERVER_IMAGE) $(SERVER_ARGS)"; \
	$(GO_DOCKER) sh -c 'timeout 60 sh -c "until curl -sf http://$(CONTRACT_SERVER):9090/readyz >/dev/null; do sleep 0.1; done" \
		|| { echo "$(CONTRACT_SERVER) not ready after 60s"; exit 1; }; \
		go test -tags wsmontest -count=1 -v -timeout 30m $(if $(SHORT),-short,) ./test/client/contract/'

# Fault-injection suite: client against a server image (SERVER_IMAGE) behind
# toxiproxy and netem. SHORT=1 passes -short.
TOXIPROXY_IMAGE?=ghcr.io/shopify/toxiproxy:2.13.0
NETEM_IMAGE?=alpine:3.22
FAULT_NET=wsmon-fault
FAULT_SERVER=wsmon-fault-server
FAULT_TOXIPROXY=wsmon-fault-toxiproxy
FAULT_NETEM=wsmon-fault-netem
FAULT_TEARDOWN=docker rm -f $(FAULT_NETEM) $(FAULT_TOXIPROXY) $(FAULT_SERVER) >/dev/null 2>&1; docker network rm $(FAULT_NET) >/dev/null 2>&1
test-fault: GO_DOCKER_ENV=--network $(FAULT_NET) --name wsmon-fault-runner \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-e WSMON_FAULT_TOXIPROXY_API=http://$(FAULT_TOXIPROXY):8474 \
	-e WSMON_FAULT_TOXIPROXY_HOST=$(FAULT_TOXIPROXY) \
	-e WSMON_FAULT_UPSTREAM=$(FAULT_SERVER):8080 \
	-e WSMON_FAULT_NETEM_CONTAINER=$(FAULT_NETEM) \
	-e WSMON_CONTRACT_TOKEN=$(CONTRACT_TOKEN) \
	-e WSMON_CONTRACT_SERVER_CONTAINER=$(FAULT_SERVER)
test-fault: $(if $(filter $(LOCAL_IMAGE),$(SERVER_IMAGE)),image-local,)
	@$(FAULT_TEARDOWN); \
	trap 'rc=$$?; [ $$rc -eq 0 ] || { echo "--- $(FAULT_SERVER) logs (tail) ---"; docker logs --tail 50 $(FAULT_SERVER) 2>&1; }; \
		docker unpause $(FAULT_SERVER) >/dev/null 2>&1; $(FAULT_TEARDOWN); exit $$rc' EXIT; \
	docker network create $(FAULT_NET) >/dev/null || exit 1; \
	docker run -d --name $(FAULT_SERVER) --network $(FAULT_NET) \
		-e WSMON_AUTH_TOKEN=$(CONTRACT_TOKEN) \
		-e WSMON_DRAIN_DELAY=1s \
		-e WSMON_SHUTDOWN_TIMEOUT=5s \
		-e WSMON_NAME=fault-server \
		-e WSMON_LABELS=zone=fault-az \
		$(SERVER_IMAGE) $(SERVER_ARGS) >/dev/null || exit 1; \
	echo "server image: $(SERVER_IMAGE) $(SERVER_ARGS)"; \
	docker run -d --name $(FAULT_TOXIPROXY) --network $(FAULT_NET) $(TOXIPROXY_IMAGE) >/dev/null || exit 1; \
	docker run -d --name $(FAULT_NETEM) --network container:$(FAULT_SERVER) --cap-add NET_ADMIN \
		$(NETEM_IMAGE) sh -c 'apk add --no-cache iproute2 >/dev/null && touch /ready && exec sleep infinity' >/dev/null || exit 1; \
	for i in $$(seq 1 120); do docker exec $(FAULT_NETEM) test -f /ready 2>/dev/null && break; sleep 0.5; done; \
	docker exec $(FAULT_NETEM) tc -V || { echo "netem sidecar: iproute2 tc not available"; exit 1; }; \
	$(GO_DOCKER) sh -c 'timeout 60 sh -c "until curl -sf http://$(FAULT_SERVER):9090/readyz >/dev/null && curl -sf http://$(FAULT_TOXIPROXY):8474/version >/dev/null; do sleep 0.1; done" \
		|| { echo "server or toxiproxy not ready after 60s"; exit 1; }; \
		go test -count=1 -v -timeout 30m $(if $(SHORT),-short,) ./test/client/fault/'

# Long-running soak tests. Tune with SOAK_CONNS and SOAK_DURATION.
SOAK_CONNS?=2000
SOAK_DURATION?=5m
test-soak: GO_DOCKER_ENV=-e WSMON_SOAK_CONNS=$(SOAK_CONNS) -e WSMON_SOAK_DURATION=$(SOAK_DURATION)
test-soak:
	$(GO_DOCKER) sh -c 'go test -tags soak -run TestSoak01 -count=1 -v -timeout 0 ./test/server/integration/ && \
		go test -tags soak -run "TestSoak0[24]" -count=1 -v -timeout 0 ./test/client/integration/'

lint:
	$(GO_DOCKER) sh -c 'go vet ./... && go vet -tags soak ./... && go vet -tags wsmontest ./... && { test -z "$$(gofmt -l .)" || { gofmt -l .; echo "unformatted files; run make fmt"; exit 1; }; }'

fmt:
	$(GO_DOCKER) go fmt ./...

tidy:
	$(GO_DOCKER) go mod tidy

# ---------------------------------------------------------------------------
# Binaries.

build-local:
	$(GO_DOCKER) env CGO_ENABLED=0 GOOS=$(HOST_OS) GOARCH=$(HOST_ARCH) go build -trimpath -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/wsmon

build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64: build-%:
	$(GO_DOCKER) env CGO_ENABLED=0 GOOS=$(word 1,$(subst -, ,$*)) GOARCH=$(word 2,$(subst -, ,$*)) \
		go build -trimpath -ldflags="$(LDFLAGS)" -o $(BINARY)-$* ./cmd/wsmon

build-all-archs: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64

# Run both roles locally: `make run-server` in one terminal, `make run-client`
# (MODE=pingpong|burst|idle) in another. Client metrics are on :9091.
MODE?=pingpong
CLIENT_METRICS_ADDR?=:9091
run-server: build-local
	WSMON_NAME=local WSMON_LOG_LEVEL=debug ./$(BINARY) server

run-client: build-local
	WSMON_MODE=$(MODE) WSMON_URL=ws://localhost:8080/__synthetic/ws WSMON_METRICS_ADDR=$(CLIENT_METRICS_ADDR) WSMON_LOG_LEVEL=debug ./$(BINARY) client

clean:
	rm -f $(BINARY) $(BINARY)-*
	rm -rf $(CHART_DIST)
	docker rmi $(LOCAL_IMAGE) || true
	docker buildx rm $(BUILDER) || true

info:
	@echo "Supported platforms: $(PLATFORMS)"
	@echo "Image: $(IMAGE):$(VERSION)"
	@echo "Go build image: $(GO_IMAGE)"
	@echo "Server image for Docker suites: $(SERVER_IMAGE) $(SERVER_ARGS)"
	@docker run --rm $(GO_IMAGE) go version
	@docker --version
