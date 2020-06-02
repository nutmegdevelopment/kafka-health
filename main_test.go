package main

import (
	"testing"
)

var kafkaURL = []string{"b-1.agrob-the-orc.g5cim7.c3.kafka.eu-west-1.amazonaws.com:9092, b-2.agrob-the-orc.g5cim7.c3.kafka.eu-west-1.amazonaws.com:9092, b-3.agrob-the-orc.g5cim7.c3.kafka.eu-west-1.amazonaws.com:9092"}

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
