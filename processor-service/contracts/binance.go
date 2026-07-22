package contracts

import "encoding/json"

type BinanceTrade struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
}

type WindowState struct {
	WindowStart int64          `json:"window_start"`
	Trades      []BinanceTrade `json:"trades"`
}

type TradeCodec struct{}

func (c *TradeCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *TradeCodec) Decode(data []byte) (interface{}, error) {
	var trade BinanceTrade
	err := json.Unmarshal(data, &trade)
	return trade, err
}

type WindowStateCodec struct{}

func (c *WindowStateCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *WindowStateCodec) Decode(data []byte) (interface{}, error) {
	var state WindowState
	err := json.Unmarshal(data, &state)
	return &state, err
}

type AggregatedTrade struct {
	Symbol      string  `json:"symbol"`
	TotalVolume float64 `json:"total_volume"`
	TradeCount  int     `json:"trade_count"`
	WindowStart int64   `json:"window_start"`
	WindowEnd   int64   `json:"window_end"`
}

func (c *AggregatedTrade) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *AggregatedTrade) Decode(data []byte) (interface{}, error) {
	var agg AggregatedTrade
	err := json.Unmarshal(data, &agg)
	return &agg, err
}
