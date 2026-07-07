package main

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

func kafkaWriter(kafkaURL string, kafkaTopic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	fmt.Println("wtf yuhoooooo")
}
