BINARY   := shunt
IMAGE    ?= shunt:dev
PLATFORM ?= linux/amd64

.PHONY: build test vet fmt lint docker run tidy clean

build: ## Build the static binary
	CGO_ENABLED=0 go build -trimpath -o $(BINARY) ./cmd/shunt

test: ## Run unit tests
	go test ./...

vet: ## go vet
	go vet ./...

fmt: ## Format the tree
	gofmt -l -w .

lint: ## gofmt + vet check (CI gate)
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	go vet ./...

docker: ## Build the container image (set PLATFORM=linux/arm64 to cross-build)
	podman build --platform $(PLATFORM) -t $(IMAGE) .

run: build ## Build + run (expects SHUNT_* env)
	./$(BINARY)

tidy: ## go mod tidy
	go mod tidy

clean:
	rm -f $(BINARY)
