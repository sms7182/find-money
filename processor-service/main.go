package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"processor-service/contracts"

	"github.com/lovoo/goka"
)

var (
	brokers                     = []string{"localhost:9092"}
	topic           goka.Stream = "raw-trades"
	group           goka.Group  = "aggregate-group-v2"
	tmc             *goka.TopicManagerConfig
	whaleThresholds = map[string]float64{
		"BTCUSDT": 50.0,
		"ETHUSDT": 500.0,
		"SOLUSDT": 2000.0,
	}
)

func init() {
	cfg := goka.DefaultConfig()
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1

	tm, err := goka.NewTopicManager(brokers, cfg, tmc)
	if err != nil {
		log.Fatalf("error creating topic manager :%v", err)
	}

	err = tm.EnsureStreamExists("aggregated-trades", 8)
	if err != nil {
		log.Fatalf("Error creating output topic:%v", err)
	}
	tm.Close()
}

func main() {

	cb := func(ctx goka.Context, msg interface{}) {
		trade := msg.(contracts.BinanceTrade)

		var windowState *contracts.WindowState
		if val := ctx.Value(); val != nil {
			windowState = val.(*contracts.WindowState)
		}

		if windowState == nil {
			windowState = &contracts.WindowState{
				WindowStart: trade.TradeTime,
				Trades:      []contracts.BinanceTrade{trade},
			}
		}

		windowDuration := int64(10 * 1000)

		if trade.TradeTime-windowState.WindowStart < windowDuration {
			windowState.Trades = append(windowState.Trades, trade)
			ctx.SetValue(windowState)
		} else {
			var totalVolume float64
			for _, t := range windowState.Trades {
				qty, _ := strconv.ParseFloat(t.Quantity, 64)
				totalVolume += qty

			}
			threshold, exists := whaleThresholds[trade.Symbol]
			if !exists {
				threshold = 1000.0
			}

			if totalVolume >= threshold {
				log.Printf("fuck fuck fuck  Symbol: %s | Total Vol in 10s: %.2f | Trades Count: %d",
					trade.Symbol, totalVolume, len(windowState.Trades))

				aggTrade := contracts.AggregatedTrade{
					Symbol:      trade.Symbol,
					TotalVolume: totalVolume,
					TradeCount:  len(windowState.Trades),
					WindowStart: windowState.WindowStart,
					WindowEnd:   trade.TradeTime,
				}
				ctx.Emit("aggregated-trades", ctx.Key(), aggTrade)
			}

			windowState.WindowStart = trade.TradeTime
			windowState.Trades = []contracts.BinanceTrade{trade}
			ctx.SetValue(windowState)
		}

		log.Printf("key =%s | added trade for symbol: %v", ctx.Key(), trade.Symbol)
	}

	g := goka.DefineGroup(
		group,
		goka.Input(topic, new(contracts.TradeCodec), cb),
		goka.Persist(new(contracts.WindowStateCodec)),
		goka.Output("aggregated-trades", new(contracts.AggregatedTrade)),
	)
	tmBuilder := goka.TopicManagerBuilderWithConfig(goka.DefaultConfig(), tmc)

	p, err := goka.NewProcessor(brokers, g,
		goka.WithTopicManagerBuilder(tmBuilder),
	)
	if err != nil {
		log.Fatalf("error creating processor:%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})

	go func() {
		defer close(done)
		log.Println("Goka processor is starting and listening to kafka ....")
		if err := p.Run(ctx); err != nil {
			log.Fatalf("error running processor :%v", err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	log.Println("shutting down processor ....")
	cancel()
	<-done
}
