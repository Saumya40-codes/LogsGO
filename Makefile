.PHONY: proto

proto:
	cd ./api/grpc/proto && \
	protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. logs.proto && \
	mv logs*.go ../pb/

build:
	go build -o logsGo ./cmd/logsGo

tests:
	go test -v ./...
	
start-react-app:
	cd ./pkg/ui && \
	npm run dev