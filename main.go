package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

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
)

//Config is the struct that will be used to unmarshal the json config file
//containing the Slack URL
type Config struct {
	URL string `json:"slackURL"`
}

//SlackMessage is the struct that we will post to Incoming Slack Webhook URL
type SlackMessage struct {
	Message string `json:"text"`
}

//slackNotify does a HTTP POST to the Incoming Webhook Integration in your Slack Team
func slackNotify(errorMessage string, slackURL string) {
	postParams := SlackMessage{fmt.Sprintf("Kafka Health Check failed: %v", errorMessage)}

	message, err := json.Marshal(postParams)
	if err != nil {
		log.Printf("Error in creating POST Message. Error : %v", err)
		return
	}
	v := url.Values{"payload": {string(message)}}
	_, err = http.PostForm(slackURL, v)
	if err != nil {
		log.Printf("Error in sending Slack Notification. Error : %v", err)
	}
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func promMetrics() {
	// Register custom metrics
	prometheus.MustRegister(producerSuccess)
	prometheus.MustRegister(consumerSuccess)
	prometheus.MustRegister(inSyncSuccess)
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

	p1 := make(chan string)
	c1 := make(chan string)

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
	// Get config path for Slack URL from env var
	configPath, error := os.LookupEnv("CONFIG_PATH")
	if error != true {
		log.Fatal("CONFIG_PATH has not been set.")
		return
	}
	// Get Slack URL from file
	file, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalln("Cannot read Slack URL configuration file:", err)
	}
	data := Config{}
	_ = json.Unmarshal([]byte(file), &data)
	slackURL := data.URL

	// Start produce / consume loop
	for {
		go consume(c1, kafkaURL, topic, slackURL)
		go produce(p1, kafkaURL, topic, slackURL)

		// Pull in messages from channels
		cMsg := <-c1
		pMsg := <-p1

		// Compare produced && consumed messages
		if pMsg != cMsg {
			noMatch := "COMPARE: Producer and consumer messages do not match."
			log.Println(noMatch)
			// Let Prometheus know we are not in sync
			inSyncSuccess.Set(0)
			// Try to catch up by comsuming again
			log.Println("COMPARE: Launching another consumer to catch up ...")
			go consume(c1, kafkaURL, topic, slackURL)
			cMsg := <-c1
			if pMsg != cMsg {
				log.Println("COMPARE: Producer and consumer are out of sync.")
				// Let Prometheus know we are not in sync
				inSyncSuccess.Set(0)
			} else {
				log.Println("COMPARE: In sync: resuming normal cycle ...")
				// Let Prometheus know we are not in sync
				inSyncSuccess.Set(1)
			}
		} else {
			log.Println("COMPARE: Producer and consumer messages matched successfully.")
			// Let Prometheus know we are not in sync
			inSyncSuccess.Set(1)
		}
		time.Sleep(10 * time.Second)
	}
}

func produce(p1 chan<- string, kafkaURL string, topic string, slackURL string) {
	// Configure writer
	writer := newKafkaWriter(kafkaURL, topic)

	log.Println("PRODUCER: Producing health check message ...")
	// Produce message
	for i := 0; i < 1; i++ {
		uuid := fmt.Sprint(uuid.New())
		dt := time.Now()
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%v", uuid)),
			Value: []byte(fmt.Sprintf(dt.Format("01-02-2006::15:04:05.00"))),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Println("PRODUCER:", err)
			slackNotify(fmt.Sprintln(err), slackURL)
			producerSuccess.Set(0)
			// close writer upon exit
			writer.Close()
			p1 <- fmt.Sprintf("%s", err)
		} else {
			log.Println("PRODUCER: Key:", string(msg.Key), "Value:", string(msg.Value))
			producerSuccess.Set(1)
			// close writer upon exit
			writer.Close()
			p1 <- string(msg.Value)
		}
	}
}

func consume(c1 chan<- string, kafkaURL string, topic string, slackURL string) {
	// Configure reader
	reader := getKafkaReader(kafkaURL, topic, "healthcheck")
	log.Println("CONSUMER: Consuming health check message ...")
	// Consume message
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("CONSUMER:", err)
			slackNotify(fmt.Sprintln(err), slackURL)
			consumerSuccess.Set(0)
			reader.Close()
		} else {
			log.Printf("CONSUMER: Consumed message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			consumerSuccess.Set(1)
			c1 <- string(m.Value)
			reader.Close()
		}
	}
}
