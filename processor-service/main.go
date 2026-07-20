package main

import (
	"log"
	"processor-service/contracts"

	"github.com/lovoo/goka"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "raw-trades"
	groupB  goka.Group  = "grp-btc"
	tmc     *goka.TopicManagerConfig
)

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}
func main() {

    cb:=func(ctx goka.Context,msg interface{}){
		var binanceTrades []contracts.BinanceTrade
		if val:=ctx.Value();val!=nil{
			binanceTrades=val.([]contracts.BinanceTrade)
		}
		binanceTrades = append(binanceTrades, msg.(contracts.BinanceTrade))
		ctx.SetValue(binanceTrades)
		log.Printf("key =%s added binance %v",ctx.Key(),msg.(contracts.BinanceTrade).)
	}
}
