# Build stage using Go 1.23
FROM golang:1.23-alpine AS builder

WORKDIR /FLS-event

# Add and download dependencies
COPY go.mod ./
RUN go mod download

# Copy the full source
COPY . .

# Build the binary (change path if needed)
RUN go build -o eventConsumer .

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /

# Copy binary
COPY --from=builder /FLS-event .

EXPOSE 8080

CMD ["./eventConsume"]