package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	fmt.Println("wtf yuhoooooao")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	kafkaURL := "localhost:9092"
	kafkaTopic := "test-raw-money"
	conn, err := kafka.DialLeader(ctx, "tcp", "localhost:9092", kafkaTopic, 0)
	if err != nil {
		panic("has error")

	}
	conn.Close()
	writer := kafkaWriter(kafkaURL, kafkaTopic)
	defer writer.Close()

	vlbytes, _ := json.Marshal("okkkkkkkkk")
	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("address-ok")),
		Value: vlbytes,
	}
	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		fmt.Println("has error,%s", err)
	}
	fmt.Println("without error")

}
