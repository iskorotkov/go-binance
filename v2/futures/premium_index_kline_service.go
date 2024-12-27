package futures

import (
	"context"
	"fmt"
	"net/http"

	"github.com/buger/jsonparser"
)

// PremiumIndexKlinesService list klines
type PremiumIndexKlinesService struct {
	c         *Client
	symbol    string
	interval  string
	limit     *int
	startTime *int64
	endTime   *int64
}

// Symbol sets symbol
func (piks *PremiumIndexKlinesService) Symbol(symbol string) *PremiumIndexKlinesService {
	piks.symbol = symbol
	return piks
}

// Interval set interval
func (piks *PremiumIndexKlinesService) Interval(interval string) *PremiumIndexKlinesService {
	piks.interval = interval
	return piks
}

// Limit set limit
func (piks *PremiumIndexKlinesService) Limit(limit int) *PremiumIndexKlinesService {
	piks.limit = &limit
	return piks
}

// StartTime set startTime
func (piks *PremiumIndexKlinesService) StartTime(startTime int64) *PremiumIndexKlinesService {
	piks.startTime = &startTime
	return piks
}

// EndTime set endTime
func (piks *PremiumIndexKlinesService) EndTime(endTime int64) *PremiumIndexKlinesService {
	piks.endTime = &endTime
	return piks
}

// Do send request
func (piks *PremiumIndexKlinesService) Do(ctx context.Context, opts ...RequestOption) (res []*Kline, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/premiumIndexKlines",
	}
	r.setParam("symbol", piks.symbol)
	r.setParam("interval", piks.interval)
	if piks.limit != nil {
		r.setParam("limit", *piks.limit)
	}
	if piks.startTime != nil {
		r.setParam("startTime", *piks.startTime)
	}
	if piks.endTime != nil {
		r.setParam("endTime", *piks.endTime)
	}
	data, _, err := piks.c.callAPI(ctx, r, opts...)
	if err != nil {
		return []*Kline{}, err
	}
	_, err = jsonparser.ArrayEach(data, func(elem []byte, _ jsonparser.ValueType, _ int, _ error) {
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
			case 6:
				res[elemIdx].CloseTime, _ = jsonparser.ParseInt(field)
			}
		})
	})
	if err != nil {
		return []*Kline{}, fmt.Errorf("parse json: %w", err)
	}
	return res, nil
}
