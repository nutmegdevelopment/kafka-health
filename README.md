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

Optional:

* LOG_LEVEL - log level

## Health Check Topic

By default:

* 9 partitions; writing to all partitions using random UUID keys and Murmur2Balancer - this ensures we test all brokers

## Metrics

Metrics are served on `/metrics`, the following metrics are exported:

```
producerSuccess - writer succeeds to produce a message
producerFailure - writer fails to produce a message
consumerSuccess - reader succeeds to read a message
consumerFailure - reader fails to read a message
inSyncSuccess - writer and reader messages are equal
inSyncFailure - writer and reader messages are not equal, i.e. consumer group could have fallen behind, can be indicative of issues
producerCxSuccess - writer connection success
producerCxFailure - writer connection failure
consumerCxSuccess - reader connection success
consumerCxFailure - reader connection failure
```
