package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
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

	var wg sync.WaitGroup
	wg.Add(2)

	go consume(&wg, kafkaURL, topic)
	go produce(&wg, kafkaURL, topic)

	wg.Wait()
	log.Println("All done.")
}

func produce(wg *sync.WaitGroup, kafkaURL string, topic string) {
	defer wg.Done()
	writer := newKafkaWriter(kafkaURL, topic)
	// close writer upon exit
	defer writer.Close()
	log.Println("PRODUCER: Starting producing ...")
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
		}
	}
}

func consume(wg *sync.WaitGroup, kafkaURL string, topic string) {
	// Need to call this explicitly because of the for loop...
	// Better way?
	// defer wg.Done()
	// Consume messages
	log.Println("CONSUMER: Starting consuming ...")
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
		log.Printf("Successfully set offset to LastOffset")
	}
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
			wg.Done()
		} else {
			log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			log.Printf("Closing reader")
			defer reader.Close()
			wg.Done()
		}
	}
}
