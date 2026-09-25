.PHONY: lint vet test build

lint:
	golangci-lint run

vet:
	go vet ./...

test:
	go test -timeout 30m ./...

build:
	go build -o bin/gateway ./cmd/gateway
