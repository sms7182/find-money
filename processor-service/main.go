package main

import (
	"log"
	"processor-service/contracts"

	"github.com/lovoo/goka"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "raw-trades"
	group   goka.Group  = "aggregate-group"
	tmc     *goka.TopicManagerConfig
)

func init() {

	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("error creating topic manager :%v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists("aggregated-trades", 8)
	if err != nil {
		log.Fatalf("Error creating output topic:%v", err)
	}
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
			windowState.WindowStart = trade.TradeTime
			windowState.Trades = []contracts.BinanceTrade{trade}
			ctx.SetValue(windowState)
		}
		log.Printf("key =%s added binance %v", ctx.Key(), msg.(contracts.BinanceTrade).Symbol)

	}

	goka.DefineGroup(
		group,
		goka.Input(topic, new(contracts.TradeCodec), cb),
		goka.Persist(new(contracts.WindowStateCodec)),
		goka.Output("aggregated-trades", new(contracts.AggregatedTrade)),
	)
}
