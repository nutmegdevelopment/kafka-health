# Kafka Health

This program:

1) Wakes up
2) Produces a message to Kafka
3) Consumes the message at the latest offset
4) Compares the two messages

## Configuration

Requires:

* `KAFKA_URL` - list of brokers
* `KAFKA_TOPIC` - health check topic to be used

## Health Check Topic

By default:

* Single partition