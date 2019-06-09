package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
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
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
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

	go consume(c1, kafkaURL, topic)
	go produce(p1, kafkaURL, topic)

	pMsg := <-c1
	cMsg := <-p1

	if pMsg != cMsg {
		log.Fatalln("Producer and consumer messages do not match.")
	} else {
		log.Println("Producer and consumer messages matched successfully.")
	}

	log.Println("All done.")
}

func produce(p1 chan<- string, kafkaURL string, topic string) {
	writer := newKafkaWriter(kafkaURL, topic)
	// close writer upon exit
	defer writer.Close()
	log.Println("PRODUCER: Producing health check message ...")
	for i := 0; i < 1; i++ {
		dt := time.Now()
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i)),
			Value: []byte(fmt.Sprintf(dt.Format("01-02-2006 15:04:05.00"))),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatalln("PRODUCER:", err)
		} else {
			log.Println("PRODUCER: Key:", string(msg.Key))
			log.Println("PRODUCER: Value:", string(msg.Value))
			p1 <- string(msg.Value)
		}
	}
}

func consume(c1 chan<- string, kafkaURL string, topic string) {
	// Need to call this explicitly because of the for loop...
	// Better way?
	// defer wg.Done()
	// Consume messages
	log.Println("CONSUMER: Consuming health check message from LastOffset ...")
	// Configure reader
	rConf := kafka.ReaderConfig{
		Brokers:   []string{kafkaURL},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e8, // 1000MB
	}
	reader := kafka.NewReader(rConf)

	err := reader.SetOffset(kafka.LastOffset)
	if err != nil {
		log.Fatalln(err)
	} else {
		log.Printf("CONSUMER: Successfully set offset to LastOffset")
	}
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Printf("CONSUMER: consumed message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			defer reader.Close()
			c1 <- string(m.Value)
		}
	}
}
