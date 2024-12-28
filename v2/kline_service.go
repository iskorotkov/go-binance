package binance

import (
	"context"
	"fmt"
	"net/http"

	"github.com/buger/jsonparser"
)

// KlinesService list klines
type KlinesService struct {
	c         *Client
	symbol    string
	interval  string
	limit     *int
	startTime *int64
	endTime   *int64
}

// Symbol set symbol
func (s *KlinesService) Symbol(symbol string) *KlinesService {
	s.symbol = symbol
	return s
}

// Interval set interval
func (s *KlinesService) Interval(interval string) *KlinesService {
	s.interval = interval
	return s
}

// Limit set limit
func (s *KlinesService) Limit(limit int) *KlinesService {
	s.limit = &limit
	return s
}

// StartTime set startTime
func (s *KlinesService) StartTime(startTime int64) *KlinesService {
	s.startTime = &startTime
	return s
}

// EndTime set endTime
func (s *KlinesService) EndTime(endTime int64) *KlinesService {
	s.endTime = &endTime
	return s
}

// Do send request
func (s *KlinesService) Do(ctx context.Context, opts ...RequestOption) (res []*Kline, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/api/v3/klines",
	}
	r.setParam("symbol", s.symbol)
	r.setParam("interval", s.interval)
	if s.limit != nil {
		r.setParam("limit", *s.limit)
	}
	if s.startTime != nil {
		r.setParam("startTime", *s.startTime)
	}
	if s.endTime != nil {
		r.setParam("endTime", *s.endTime)
	}
	buf, err := s.c.callAPIPooled(ctx, r, opts...)
	if err != nil {
		return []*Kline{}, err
	}
	defer s.c.pool.Put(buf)
	_, err = jsonparser.ArrayEach(buf.Bytes(), func(elem []byte, _ jsonparser.ValueType, _ int, _ error) {
		res = append(res, &Kline{})
		elemIdx := len(res) - 1
		fieldIdx := -1
		_, _ = jsonparser.ArrayEach(elem, func(field []byte, _ jsonparser.ValueType, _ int, _ error) {
			fieldIdx++
			switch fieldIdx {
			case 0:
				res[elemIdx].OpenTime, _ = jsonparser.ParseInt(field)
			case 1:
				res[elemIdx].Open = string(field)
			case 2:
				res[elemIdx].High = string(field)
			case 3:
				res[elemIdx].Low = string(field)
			case 4:
				res[elemIdx].Close = string(field)
			case 5:
				res[elemIdx].Volume = string(field)
			case 6:
				res[elemIdx].CloseTime, _ = jsonparser.ParseInt(field)
			case 7:
				res[elemIdx].QuoteAssetVolume = string(field)
			case 8:
				res[elemIdx].TradeNum, _ = jsonparser.ParseInt(field)
			case 9:
				res[elemIdx].TakerBuyBaseAssetVolume = string(field)
			case 10:
				res[elemIdx].TakerBuyQuoteAssetVolume = string(field)
			}
		})
	})
	if err != nil {
		return []*Kline{}, fmt.Errorf("parse json: %w", err)
	}
	return res, nil
}

// Kline define kline info
type Kline struct {
	OpenTime                 int64  `json:"openTime"`
	Open                     string `json:"open"`
	High                     string `json:"high"`
	Low                      string `json:"low"`
	Close                    string `json:"close"`
	Volume                   string `json:"volume"`
	CloseTime                int64  `json:"closeTime"`
	QuoteAssetVolume         string `json:"quoteAssetVolume"`
	TradeNum                 int64  `json:"tradeNum"`
	TakerBuyBaseAssetVolume  string `json:"takerBuyBaseAssetVolume"`
	TakerBuyQuoteAssetVolume string `json:"takerBuyQuoteAssetVolume"`
}
