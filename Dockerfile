FROM golang:alpine AS builder

WORKDIR /app

COPY . .

ENV USER=nutmeg
ENV UID=10001
RUN adduser \
		--disabled-password \
		--gecos "" \
		--home "/nonexistent" \
		--shell "/sbin/nologin" \
		--no-create-home \
		--uid "${UID}" \
		"${USER}" && \
	apk update && apk add --no-cache git && \
	go mod download && \
	go mod verify && \
    CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -ldflags="-w -s" -o kafka-health -v

FROM scratch

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=alpine:latest /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/kafka-health /app/kafka-health
USER nutmeg:nutmeg

ENTRYPOINT ["/app/kafka-health"]
