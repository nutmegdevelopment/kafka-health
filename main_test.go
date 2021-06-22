package main

import (
	"testing"
)

var kafkaURL = []string{"b-1.c3.kafka.eu-west-1.amazonaws.com:9092, b-2.c3.kafka.eu-west-1.amazonaws.com:9092, b-3.c3.kafka.eu-west-1.amazonaws.com:9092"}

var topic = "healthcheck"

func TestCompareDifferentString(t *testing.T) {
	err := compare("same string", "not same string", kafkaURL, topic)

	if err != false {
		t.Fatalf("Expected `false` but returned %v", err)
	}
}

func TestCompareSameString(t *testing.T) {
	err := compare("same string", "same string", kafkaURL, topic)

	if err != true {
		t.Fatalf("Expected `true` but returned %v", err)
	}
}
