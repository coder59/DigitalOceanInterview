# Stage 1: build
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

ENV GOTOOLCHAIN=local

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /api-service ./cmd/api

# Stage 2: runtime
FROM scratch

COPY --from=builder /api-service /api-service

EXPOSE 8080

ENTRYPOINT ["/api-service"]
