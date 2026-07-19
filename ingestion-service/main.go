package main

import (
	"context"
	"encoding/json"
	models "ingestion-service/ingestion"
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

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	kafkaURL := "localhost:9092"
	kafkaTopic := "raw-trades"

	kafkaCtx, kafkaCancel := context.WithTimeout(context.Background(), 10*time.Second)

	conn, err := kafka.DialLeader(
		kafkaCtx,
		"tcp",
		kafkaURL,
		kafkaTopic,
		0,
	)

	kafkaCancel()

	if err != nil {
		log.Fatalf("Kafka connection error: %v", err)
	}

	conn.Close()
	log.Println("Kafka connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := kafkaWriter(kafkaURL, kafkaTopic)
	defer writer.Close()

	socketURL := "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade/dogeusdt@aggTrade"

	sconn, _, err := websocket.DefaultDialer.Dial(socketURL, nil)
	if err != nil {
		log.Fatalf("Websocket dial error: %v", err)
	}
	defer sconn.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			_, message, err := sconn.ReadMessage()
			if err != nil {
				log.Printf("Websocket read error: %v", err)
				return
			}

			var event models.BinanceCombinedResponse
			if err := json.Unmarshal(message, &event); err != nil {
				log.Printf("JSON unmarshal error: %v", err)
				continue
			}
			b, err := json.Marshal(event.Data)
			if err != nil {
				log.Printf("json marshal of binance message data has error %v", err)
				continue
			}
			msg := kafka.Message{
				Key:   []byte(event.Data.Symbol),
				Value: b,
			}

			if err := writer.WriteMessages(ctx, msg); err != nil {
				log.Printf("Kafka write error: %v", err)
				return
			}

			log.Printf(
				"[%s] Price: %s | Quantity: %s | Time: %d",
				event.Data.Symbol,
				event.Data.Price,
				event.Data.Quantity,
				event.Data.TradeTime,
			)
		}
	}()

	for {
		select {
		case <-done:
			log.Println("Websocket closed")
			return

		case <-interrupt:
			log.Println("Interrupt received, closing gracefully...")

			cancel()

			err := sconn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)

			if err != nil {
				log.Printf("Websocket close error: %v", err)
			}

			select {
			case <-done:
			case <-time.After(time.Second):
			}

			return
		}
	}
}
