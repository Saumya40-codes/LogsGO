FROM golang:1.27.0-alpine3.24 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o logsGo ./cmd/logsGo

FROM alpine:3.24 AS final
WORKDIR /app
COPY --from=builder /app/logsGo .
ENTRYPOINT ["./logsGo"]
