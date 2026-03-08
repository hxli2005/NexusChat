# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder
ARG SERVICE
WORKDIR /app

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOFLAGS=-mod=vendor go build -o /out/service ./cmd/${SERVICE}

FROM alpine:3.20
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=builder /out/service /app/service
USER app
ENTRYPOINT ["/app/service"]
