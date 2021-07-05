package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/viper"

	joonix "github.com/joonix/log"
	log "github.com/sirupsen/logrus"
)

var (
	producerSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_producer_success",
			Help: "Producer succeeded to produce message to Kafka.",
		},
	)

	producerFailure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_producer_failure",
			Help: "Producer failed to produce message to Kafka.",
		},
	)

	consumerSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_consumer_success",
			Help: "Consumer succeeded to produce message to Kafka.",
		},
	)

	consumerFailure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_consumer_failure",
			Help: "Consumer failed to produce message to Kafka.",
		},
	)

	inSyncSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_in_sync_success",
			Help: "Producer and consumer are in sync",
		},
	)

	inSyncFailure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_in_sync_failure",
			Help: "Producer and consumer are NOT in sync",
		},
	)

	producerCxSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_producer_cx_success",
			Help: "Producer succeeded in connecting to the Kafka cluster",
		},
	)

	producerCxFailure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_producer_cx_failure",
			Help: "Producer failed in connecting to the Kafka cluster",
		},
	)

	consumerCxSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_consumer_cx_success",
			Help: "Consumer succeeded in connecting to the Kafka cluster",
		},
	)

	consumerCxFailure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_health_check_consumer_cx_failure",
			Help: "Consumer failed in connecting to the Kafka cluster",
		},
	)
)

func newKafkaWriter(kafkaURL []string, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafkaURL,
		Topic:    topic,
		Balancer: kafka.Murmur2Balancer{},
	})
}

func getKafkaReader(kafkaURL []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kafkaURL,
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
	prometheus.MustRegister(producerFailure)
	prometheus.MustRegister(consumerSuccess)
	prometheus.MustRegister(consumerFailure)
	prometheus.MustRegister(inSyncSuccess)
	prometheus.MustRegister(inSyncFailure)
	prometheus.MustRegister(producerCxSuccess)
	prometheus.MustRegister(producerCxFailure)
	prometheus.MustRegister(consumerCxSuccess)
	prometheus.MustRegister(consumerCxFailure)

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	log.Println("serving /metrics endpoint")

	// Start HTTP server
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func setup() ([]string, string) {
	// Log as Fluentd formatter instead of the default
	// ASCII formatter.
	log.SetFormatter(joonix.NewFormatter())

	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)

	// get configuration from file / env
	logLevel, kafkaURL, kafkaTopic := configure()

	logLevel = strings.ToUpper(logLevel)

	if logLevel == "INFO" {
		log.SetLevel(log.InfoLevel)
	} else if logLevel == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else if logLevel == "WARN" {
		log.SetLevel(log.WarnLevel)
	} else if logLevel == "FATAL" {
		log.SetLevel(log.FatalLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Debug("finished configuring logger")

	return kafkaURL, kafkaTopic
}

func configure() (string, []string, string) {
	viper.SetConfigName("config")     // name of config file (without extension)
	viper.SetConfigType("yaml")       // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("/etc/conf/") // path to look for the config file in
	viper.AddConfigPath(".")          // optionally look for config in the working directory
	err := viper.ReadInConfig()       // find and read the config file
	if err != nil {
		// handle errors reading the config file
		log.Infof("%v - using env vars", err)

		viper.AutomaticEnv()

		viper.BindEnv("log_level")
		viper.BindEnv("kafka_url")
		viper.BindEnv("kafka_topic")

		logLevel := viper.GetString("log_level")
		URLs := viper.GetString("kafka_url")
		topic := viper.GetString("kafka_topic")

		kafkaURL := strings.Split(URLs, ",")

		return logLevel, kafkaURL, topic
	}

	logLevel := viper.GetString("log_level")
	kafkaURL := viper.GetStringSlice("kafka_url")
	topic := viper.GetString("kafka_topic")

	return logLevel, kafkaURL, topic
}

func main() {
	kafkaURL, topic := setup()

	// Prometheus bits
	go promMetrics()

	loop(kafkaURL, topic)
}

func loop(kafkaURL []string, topic string) {
	ticker := time.NewTicker(1 * time.Minute)

	for range ticker.C {
		pMsg, pSuccess := produce(kafkaURL, topic)
		if pSuccess != true {
			log.Println("PRODUCER: There was an error producing to the Kafka cluster!")
			break
		}

		cMsg, cSuccess := consume(kafkaURL, topic)
		if cSuccess != true {
			log.Println("CONSUMER: There was an error consuming from the Kafka cluster!")
			break
		}

		compareResult := compare(pMsg, cMsg, kafkaURL, topic)
		if compareResult != true {
			continue
		}
	}
	backoffLoop(kafkaURL, topic)
}

func backoffLoop(kafkaURL []string, topic string) {
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
			log.Println("PRODUCER: There was an error producing to the Kafka cluster!")
			continue
		}

		cMsg, cSuccess := consume(kafkaURL, topic)
		if cSuccess != true {
			log.Println("CONSUMER: There was an error consuming from the Kafka cluster!")
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

func compare(pMsg string, cMsg string, kafkaURL []string, topic string) bool {
	// Compare produced && consumed messages
	if pMsg != cMsg {
		noMatch := "COMPARE: Producer and consumer messages do not match."
		log.Println(noMatch)

		// Try to catch up by comsuming again
		log.Println("COMPARE: Launching another consumer to catch up ...")

		c1 := make(chan string, 1)

		// compare function in its own goroutine and pass back its
		// response into channel c1
		go func() {
			cMsg, cSuccess := consume(kafkaURL, topic)
			if cSuccess != true {
				log.Println("CONSUMER: There was an error consuming from the Kafka cluster!")
			}
			c1 <- cMsg
		}()

		// Listen on our channel AND a timeout channel - which ever happens first.
		select {
		case res := <-c1:
			log.Println(res)
		case <-time.After(10 * time.Second):
			log.Println("CONSUMER: Forced timeout after 10 seconds")
		}

		if pMsg != cMsg {
			log.Println("COMPARE: Producer and consumer are out of sync.")
			// Let Prometheus know we are not in sync
			inSyncFailure.Add(1)
			return false
		}

		log.Println("COMPARE: In sync: Producer and consumer messages matched successfully.\n  Resuming normal cycle ...")
	}

	// Let Prometheus know we are in sync
	inSyncSuccess.Add(1)
	return true
}

func produce(kafkaURL []string, topic string) (string, bool) {
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

		pCxError := strings.Contains(fmt.Sprint(err), "dial tcp")
		if pCxError == true {
			producerFailure.Add(1)
			producerCxFailure.Add(1)

			return fmt.Sprint(err), false
		}

		producerFailure.Add(1)
		producerCxSuccess.Add(1)

		return fmt.Sprint(err), false
	}

	log.Println("PRODUCER: Key:", string(msg.Key), "Value:", string(msg.Value))

	producerSuccess.Add(1)
	producerCxSuccess.Add(1)

	return string(msg.Value), true
}

func consume(kafkaURL []string, topic string) (string, bool) {
	// Configure reader
	reader := getKafkaReader(kafkaURL, topic, "healthcheck")
	defer reader.Close()

	log.Println("CONSUMER: Consuming health check message ...")
	// Consume message
	m, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Println("CONSUMER:", err)

		cCxError := strings.Contains(fmt.Sprint(err), "dial tcp")
		if cCxError == true {
			consumerFailure.Add(1)
			consumerCxFailure.Add(1)
			return fmt.Sprint(err), false
		}

		consumerFailure.Add(1)
		consumerCxSuccess.Add(1)

		return fmt.Sprint(err), false
	}

	log.Printf("CONSUMER: Consumed message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

	consumerSuccess.Add(1)
	consumerCxSuccess.Add(1)

	return string(m.Value), true
}
