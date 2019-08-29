# Kafka Health

[![Go Report Card](https://goreportcard.com/badge/github.com/chris-vest/kafka-health)](https://goreportcard.com/report/github.com/chris-vest/kafka-health)

This program:

1) Wakes up
2) Produces a message to Kafka
3) Consumes the message at the latest offset
4) Compares the two messages

## Build

`GO111MODULE=ON go build`

This will build the binary, pulling in all required modules as it goes. Pew pew. :cat2:

## Configuration

Requires:

* `KAFKA_URL` - list of brokers
* `KAFKA_TOPIC` - health check topic to be used

## Health Check Topic

By default:

* Single partition

## To Do

* Tests
* Backoff on Slack notifications