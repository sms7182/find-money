package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

func kafkaWriter(kafkaURL string, kafkaTopic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type BinanceTickerEvent struct {
	EventType string `json:"e"` // Event type
	EventTime int64  `json:"E"` // Event time
	Symbol    string `json:"s"` // Symbol
	LastPrice string `json:"c"` // Last price
	BidPrice  string `json:"b"` // Best bid price
	AskPrice  string `json:"a"` // Best ask price
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	fmt.Println("wtf yuhoooooao")
	// ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	// defer cancel()

	// kafkaURL := "localhost:9092"
	// kafkaTopic := "test-raw-money"
	// conn, err := kafka.DialLeader(ctx, "tcp", "localhost:9092", kafkaTopic, 0)
	// if err != nil {
	// 	panic("has error")

	// }
	// conn.Close()
	// writer := kafkaWriter(kafkaURL, kafkaTopic)
	// defer writer.Close()

	socketURL := "wss://stream.binance.com:9443/ws/btcusdt@trade"
	sconn, _, err := websocket.DefaultDialer.Dial(socketURL, nil)
	if err != nil {
		log.Fatalf("dial error.%v", err)
	}
	defer sconn.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := sconn.ReadMessage()
			if err != nil {
				log.Printf("Read error:%v", err)
				return
			}
			var event BinanceTickerEvent
			if err := json.Unmarshal(message, &event); err != nil {
				log.Printf("JSON Unmarshal error: %v", err)
				continue
			}

			log.Printf("[%s] Price: %s | Bid: %s | Ask: %s",
				event.Symbol, event.LastPrice, event.BidPrice, event.AskPrice)
		}
	}()

	for {
		select {
		case <-done:
			return
		case <-interrupt:
			log.Println("Interrupt received, closing connection gracefully...")

			err := sconn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			if err != nil {
				log.Printf("Write close error: %v", err)
				return
			}

			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
	// vlbytes, _ := json.Marshal("okkkkkkkkk")
	// msg := kafka.Message{
	// 	Key:   []byte(fmt.Sprintf("address-ok")),
	// 	Value: vlbytes,
	// }
	// err = writer.WriteMessages(ctx, msg)
	// if err != nil {
	// 	fmt.Println("has error,%s", err)
	// }
	// fmt.Println("without error")

}
