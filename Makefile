.PHONY: lint vet test build

lint:
	golangci-lint run

vet:
	go vet ./...

test:
	go test ./...

build:
	go build -o bin/gateway ./cmd/gateway
