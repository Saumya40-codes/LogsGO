.PHONY: proto

proto:
	cd ./api/grpc/proto && \
	protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. logs.proto && \
	mv logs*.go ../pb/
