BINARY   := shunt
IMAGE    ?= shunt:dev
PLATFORM ?= linux/amd64
GOTOOLCHAIN := go1.25.12

.PHONY: build test vet fmt lint docker helm-lint kustomize run tidy clean

build: ## Build the static binary
	CGO_ENABLED=0 GOTOOLCHAIN=$(GOTOOLCHAIN) go build -trimpath -o $(BINARY) ./cmd/shunt

test: ## Run unit tests
	GOTOOLCHAIN=$(GOTOOLCHAIN) go test ./...

vet: ## go vet
	GOTOOLCHAIN=$(GOTOOLCHAIN) go vet ./...

fmt: ## Format the tree
	gofmt -l -w .

lint: ## gofmt + vet check (CI gate)
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	GOTOOLCHAIN=$(GOTOOLCHAIN) go vet ./...

docker: ## Build the container image (set PLATFORM=linux/arm64 to cross-build)
	podman build --platform $(PLATFORM) -t $(IMAGE) .

helm-lint: ## Lint the Helm chart
	helm lint charts/shunt

kustomize: ## Render the Kustomize base
	kubectl kustomize deploy/kustomize/base >/dev/null

run: build ## Build + run (expects SHUNT_* env)
	./$(BINARY)

tidy: ## go mod tidy
	GOTOOLCHAIN=$(GOTOOLCHAIN) go mod tidy

clean:
	rm -f $(BINARY)
