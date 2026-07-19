package main

import "github.com/lovoo/goka"

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

}
