FROM golang:alpine AS builder

WORKDIR /app

# Install git.
# Git is required for fetching the dependencies.
RUN apk update && apk add --no-cache git

COPY . .

RUN go mod download && \
    # Build the binary.
    CGO_ENABLED=0 GOOS=linux go build -o kafka-health -v

FROM scratch

# Copy our static executable.
COPY --from=builder /app/kafka-health /app/kafka-health

# Run the hello binary.
ENTRYPOINT ["/app/kafka-health"]