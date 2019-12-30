package main

import (
	"testing"
)

var kafkaURL = "b-1.krusk-the-barbarian.qibb1j.c3.kafka.eu-west-1.amazonaws.com:9092"

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
