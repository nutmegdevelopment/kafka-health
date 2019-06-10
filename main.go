package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	successBinary = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_health_check_success",
			Help: "Last health check success or failure.",
		},
	)
)

//SlackMessage is the struct that we will post to Incoming Slack Webhook URL
type SlackMessage struct {
	Message string `json:"text"`
}

//raiseSlackNotification does a HTTP POST to the Incoming Webhook Integration in your Slack Team
func raiseSlackNotification(errorMessage string, slackURL string) {
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

func main() {
	prometheus.MustRegister(successBinary)

	p1 := make(chan string)
	c1 := make(chan string)

	// get kafka writer using environment variables.
	kafkaURL, err := os.LookupEnv("KAFKA_URL")
	if err != true {
		log.Fatal("KAFKA_URL has not been set.")
		return
	}
	topic, err := os.LookupEnv("KAFKA_TOPIC")
	if err != true {
		log.Fatal("KAFKA_TOPIC has not been set.")
		return
	}
	slackURL, err := os.LookupEnv("SLACK_URL")
	if err != true {
		log.Fatal("SLACK_URL has not been set.")
		return
	}

	go consume(c1, kafkaURL, topic, slackURL)
	go produce(p1, kafkaURL, topic, slackURL)

	// Pull in messages from channels
	pMsg := <-c1
	cMsg := <-p1

	// Compare produced && consumed messages
	if pMsg != cMsg {
		noMatch := "COMPARE: Producer and consumer messages do not match."
		log.Fatalln(noMatch)
		successBinary.Set(0)
		raiseSlackNotification(noMatch, slackURL)
	} else {
		log.Println("COMPARE: Producer and consumer messages matched successfully.")
		successBinary.Set(1)
	}

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	log.Print("Serving /metrics endpoint.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func produce(p1 chan<- string, kafkaURL string, topic string, slackURL string) {
	// Configure writer
	writer := newKafkaWriter(kafkaURL, topic)
	// close writer upon exit
	defer writer.Close()
	log.Println("PRODUCER: Producing health check message ...")
	for i := 0; i < 1; i++ {
		uuid := fmt.Sprint(uuid.New())
		dt := time.Now()
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%v", uuid)),
			Value: []byte(fmt.Sprintf(dt.Format("01-02-2006::15:04:05.00"))),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatalln("PRODUCER:", err)
			raiseSlackNotification(fmt.Sprintln(err), slackURL)
		} else {
			log.Println("PRODUCER: Key:", string(msg.Key), "Value:", string(msg.Value))
			p1 <- string(msg.Value)
		}
	}
}

func consume(c1 chan<- string, kafkaURL string, topic string, slackURL string) {
	// Configure reader
	reader := getKafkaReader(kafkaURL, topic, "healthcheck")
	// Consume messages
	log.Println("CONSUMER: Consuming health check message ...")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
			raiseSlackNotification(fmt.Sprintln(err), slackURL)
		} else {
			log.Printf("CONSUMER: Consumed message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			defer reader.Close()
			c1 <- string(m.Value)
		}
	}
}
