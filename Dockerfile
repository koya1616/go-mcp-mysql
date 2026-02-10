FROM golang:1.24-alpine AS base
WORKDIR /app

FROM base AS deps
COPY go.mod go.sum ./
RUN go mod download

FROM deps AS builder
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags='-s -w' -o /out/mcp-mysql ./main.go

FROM base AS dev
COPY go.mod go.sum ./
RUN go mod download
COPY . .
CMD ["go", "run", "./main.go"]

FROM alpine:3.22 AS prod
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=builder /out/mcp-mysql /usr/local/bin/mcp-mysql
ENTRYPOINT ["/usr/local/bin/mcp-mysql"]
