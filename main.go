package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"
)

var (
	producerSuccess = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_health_check_producer_success",
			Help: "Producer succeeded to produce message to Kafka.",
		},
	)

	consumerSuccess = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_health_check_consumer_success",
			Help: "Consumer succeeded to consumer message from Kafka.",
		},
	)

	inSyncSuccess = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_health_check_in_sync",
			Help: "Producer and consumer are in sync",
		},
	)

	producerCxSuccess = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_health_check_producer_cx_success",
			Help: "Producer succeeded in connecting to the Kafka cluster",
		},
	)

	consumerCxSuccess = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_health_check_consumer_cx_success",
			Help: "Consumer succeeded in connecting to the Kafka cluster",
		},
	)
)

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaURL},
		GroupID:     groupID,
		Topic:       topic,
		MaxWait:     5 * time.Second,
		MaxAttempts: 1,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	})
}

func promMetrics() {
	// Register custom metrics
	prometheus.MustRegister(producerSuccess)
	prometheus.MustRegister(consumerSuccess)
	prometheus.MustRegister(inSyncSuccess)
	prometheus.MustRegister(producerCxSuccess)
	prometheus.MustRegister(consumerCxSuccess)
	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	log.Print("Serving /metrics endpoint.")
	// Start HTTP server
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func main() {
	// Prometheus bits
	go promMetrics()

	// Get Kafka URL from env var
	kafkaURL, error := os.LookupEnv("KAFKA_URL")
	if error != true {
		log.Fatal("KAFKA_URL has not been set.")
		return
	}
	// Get Kafka topic from env var
	topic, error := os.LookupEnv("KAFKA_TOPIC")
	if error != true {
		log.Fatal("KAFKA_TOPIC has not been set.")
		return
	}

	loop(kafkaURL, topic)
}

func loop(kafkaURL string, topic string) {
	ticker := time.NewTicker(1 * time.Minute)

	for range ticker.C {
		pMsg, pSuccess := produce(kafkaURL, topic)
		if pSuccess != true {
			log.Print("PRODUCER: There was an error producing to the Kafka cluster!")
			break
		}

		cMsg, cSuccess := consume(kafkaURL, topic)
		if cSuccess != true {
			log.Print("CONSUMER: There was an error consuming from the Kafka cluster!")
			break
		}

		compareResult := compare(pMsg, cMsg, kafkaURL, topic)
		if compareResult != true {
			continue
		}
	}
	backoffLoop(kafkaURL, topic)
}

func backoffLoop(kafkaURL string, topic string) {
	// We can use a ticker to get the current replica count
	// every x amount of time, with an exponential backoff
	exponentialBackOff := &backoff.ExponentialBackOff{
		InitialInterval:     1 * time.Minute,
		RandomizationFactor: 0.2,
		Multiplier:          1.5,
		MaxInterval:         12 * time.Hour,
		MaxElapsedTime:      48 * time.Hour,
		Clock:               backoff.SystemClock,
	}

	// Create the ticker
	ticker := backoff.NewTicker(exponentialBackOff)

	for range ticker.C {
		log.Println("Entering backoff...")

		pMsg, pSuccess := produce(kafkaURL, topic)
		if pSuccess != true {
			log.Print("PRODUCER: There was an error producing to the Kafka cluster!")
			continue
		}

		cMsg, cSuccess := consume(kafkaURL, topic)
		if cSuccess != true {
			log.Print("CONSUMER: There was an error consuming from the Kafka cluster!")
			continue
		}

		compareResult := compare(pMsg, cMsg, kafkaURL, topic)
		if compareResult != true {
			continue
		}

		break
	}
	log.Println("Exiting backoff...")
	loop(kafkaURL, topic)
}

func compare(pMsg string, cMsg string, kafkaURL string, topic string) bool {
	// Compare produced && consumed messages
	log.Print(pMsg)

	log.Print(cMsg)

	if pMsg != cMsg {
		noMatch := "COMPARE: Producer and consumer messages do not match."
		log.Println(noMatch)
		// Let Prometheus know we are not in sync
		inSyncSuccess.Set(0)
		// Try to catch up by comsuming again
		log.Println("COMPARE: Launching another consumer to catch up ...")
		cMsg, cSuccess := consume(kafkaURL, topic)
		if cSuccess != true {
			log.Print("COMPARE[230] CONSUMER: Caught you!")
			return false
		}

		if pMsg != cMsg {
			log.Println("COMPARE: Producer and consumer are out of sync.")
			// Let Prometheus know we are not in sync
			inSyncSuccess.Set(0)
			return false
		}

		log.Println("COMPARE: ")
		log.Println("COMPARE: In sync: Producer and consumer messages matched successfully.\n  Resuming normal cycle ...")
	}

	// Let Prometheus know we are in sync
	inSyncSuccess.Set(1)
	return true
}

func produce(kafkaURL string, topic string) (string, bool) {
	// Configure writer
	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()

	log.Println("PRODUCER: Producing health check message ...")
	// Produce message
	uuid := fmt.Sprint(uuid.New())
	dt := time.Now()
	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("Key-%v", uuid)),
		Value: []byte(fmt.Sprintf(dt.Format("01-02-2006::15:04:05.00"))),
	}
	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Println("PRODUCER:", err)

		producerSuccess.Set(0)

		pCxError := strings.Contains(fmt.Sprint(err), "dial tcp")
		if pCxError == true {
			producerSuccess.Set(0)
			producerCxSuccess.Set(0)

			return fmt.Sprint(err), false
		}

		producerSuccess.Set(0)
		producerCxSuccess.Set(1)

		return fmt.Sprint(err), false
	}

	log.Println("PRODUCER: Key:", string(msg.Key), "Value:", string(msg.Value))

	producerSuccess.Set(1)
	producerCxSuccess.Set(1)

	return string(msg.Value), true
}

func consume(kafkaURL string, topic string) (string, bool) {
	// Configure reader
	reader := getKafkaReader(kafkaURL, topic, "healthcheck1")
	defer reader.Close()

	log.Println("CONSUMER: Consuming health check message ...")
	// Consume message
	m, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Println("CONSUMER:", err)

		cCxError := strings.Contains(fmt.Sprint(err), "dial tcp")
		if cCxError == true {
			consumerSuccess.Set(0)
			consumerCxSuccess.Set(0)
			return fmt.Sprint(err), false
		}

		consumerSuccess.Set(0)
		consumerCxSuccess.Set(1)

		return fmt.Sprint(err), false
	}

	log.Printf("CONSUMER: Consumed message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

	consumerSuccess.Set(1)
	consumerCxSuccess.Set(1)

	return string(m.Value), true
}
