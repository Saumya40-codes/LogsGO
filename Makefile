proto: ## Generate Go code from protobuf
	cd ./api/grpc/proto && \
	protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. logs.proto && \
	mv logs*.go ../pb/

build: ## Build Go binary
	go build -o logsGo ./cmd/logsGo

test: ## Run unit tests only (skip Docker/e2e tests)
	@go test -short -v ./... && cat ./.github/surp.txt

tests: ## Run Go tests and show Pikachu on success
	@go test -v ./... && cat ./.github/surp.txt

query-time-test: ## Run benchmarish query time test
	LOGSGO_QUERY_TIMING=1 go test ./pkg/store -run 'TestQueryTiming(Memory|Local)Store' -v -count=1

start-react-app: ## Start React app (Vite dev server)
	cd ./pkg/ui && \
	npm run dev

build-react-app: ## Build React app
	cd ./pkg/ui && \
	npm run build

build-all: ## Build both frontend and backend
	make build-react-app && \
	make build

help:
	@echo "Available targets:"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
