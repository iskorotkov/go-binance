package json

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/bitly/go-simplejson"
	"github.com/buger/jsonparser"
	"github.com/bytedance/sonic"
	"github.com/francoispqt/gojay"
	gojson "github.com/goccy/go-json"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/simdjson-go"
	"github.com/tidwall/gjson"
	"github.com/ugorji/go/codec"
)

//go:embed testdata.json
var jsonUnmarshalBenchmarkData []byte

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

func (k *Kline) UnmarshalJSONArray(dec *gojay.Decoder) error {
	if k.OpenTime == 0 {
		if err := dec.Int64(&k.OpenTime); err != nil {
			return err
		}
		return nil
	}
	if k.Open == "" {
		if err := dec.String(&k.Open); err != nil {
			return err
		}
		return nil
	}
	if k.High == "" {
		if err := dec.String(&k.High); err != nil {
			return err
		}
		return nil
	}
	if k.Low == "" {
		if err := dec.String(&k.Low); err != nil {
			return err
		}
		return nil
	}
	if k.Close == "" {
		if err := dec.String(&k.Close); err != nil {
			return err
		}
		return nil
	}
	if k.Volume == "" {
		if err := dec.String(&k.Volume); err != nil {
			return err
		}
		return nil
	}
	if k.CloseTime == 0 {
		if err := dec.Int64(&k.CloseTime); err != nil {
			return err
		}
		return nil
	}
	var discard any
	if err := dec.Interface(&discard); err != nil {
		return err
	}
	return nil
}

type Klines []*Kline

func (k *Klines) UnmarshalJSONArray(dec *gojay.Decoder) error {
	kline := &Kline{}
	if err := dec.DecodeArray(kline); err != nil {
		return err
	}
	*k = append(*k, kline)
	return nil
}

func BenchmarkJSONUnmarshalPremiumIndexKlines(b *testing.B) {
	var res []*Kline

	b.Run("encoding/json", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			temp := make([][]any, 1000)
			dec := json.NewDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData))
			dec.UseNumber()
			if err := dec.Decode(&temp); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, len(temp))
			for i, item := range temp {
				ot, err := item[0].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := item[6].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: ct,
				}
			}
		}
	})

	b.Run("bitly/go-simplejson", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// From https://github.com/iskorotkov/go-binance/blob/a836b57116022e8221034e240bc60e9f4278fff5/v2/futures/client.go#L339.
			j, err := simplejson.NewJson(jsonUnmarshalBenchmarkData)
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			num := len(j.MustArray())
			res = make([]*Kline, num)
			for i := 0; i < num; i++ {
				item := j.GetIndex(i)
				if len(item.MustArray()) < 11 {
					b.Fatalf("invalid kline response")
				}
				res[i] = &Kline{
					OpenTime:  item.GetIndex(0).MustInt64(),
					Open:      item.GetIndex(1).MustString(),
					High:      item.GetIndex(2).MustString(),
					Low:       item.GetIndex(3).MustString(),
					Close:     item.GetIndex(4).MustString(),
					CloseTime: item.GetIndex(6).MustInt64(),
				}
			}
		}
	})

	b.Run("goccy/go-json", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			temp := make([][]any, 1000)
			dec := gojson.NewDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData))
			dec.UseNumber()
			if err := dec.Decode(&temp); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, len(temp))
			for i, item := range temp {
				ot, err := item[0].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := item[6].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: ct,
				}
			}
		}
	})

	b.Run("ugorji/go", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			temp := make([][]any, 1000)
			dec := codec.NewDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData), &codec.JsonHandle{})
			if err := dec.Decode(&temp); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, len(temp))
			for i, item := range temp {
				res[i] = &Kline{
					OpenTime:  int64(item[0].(uint64)),
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: int64(item[6].(uint64)),
				}
			}
		}
	})

	b.Run("minio/simdjson-go", func(b *testing.B) {
		var reusedJSON simdjson.ParsedJson
		var rootIter simdjson.Iter
		var rootArr simdjson.Array
		var elemArr simdjson.Array
		for i := 0; i < b.N; i++ {
			pj, err := simdjson.Parse(jsonUnmarshalBenchmarkData, &reusedJSON)
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			iter := pj.Iter()
			res = make([]*Kline, 1000)
			for {
				typ := iter.Advance()
				if typ == simdjson.TypeNone {
					break
				}
				typ, rootIter, err := iter.Root(&rootIter)
				if err != nil {
					b.Fatalf("invalid kline response")
				}
				if typ != simdjson.TypeArray {
					b.Fatalf("invalid kline response")
				}
				rootArr, err := rootIter.Array(&rootArr)
				if err != nil {
					b.Fatalf("invalid kline response")
				}
				elemIter := rootArr.Iter()
				for elem := 0; ; elem++ {
					typ := elemIter.Advance()
					if typ == simdjson.TypeNone {
						break
					}
					elemArr, err := elemIter.Array(&elemArr)
					if err != nil {
						b.Fatalf("invalid kline response")
					}
					fieldIter := elemArr.Iter()
					res[elem] = &Kline{}
				fieldsLoop:
					for field := 0; ; field++ {
						typ := fieldIter.Advance()
						if typ == simdjson.TypeNone {
							break
						}
						switch field {
						case 0:
							intVal, err := fieldIter.Int()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].OpenTime = intVal
						case 1:
							strVal, err := fieldIter.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].Open = string(strVal)
						case 2:
							strVal, err := fieldIter.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].High = string(strVal)
						case 3:
							strVal, err := fieldIter.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].Low = string(strVal)
						case 4:
							strVal, err := fieldIter.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].Close = string(strVal)
						case 6:
							intVal, err := fieldIter.Int()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].CloseTime = intVal
							break fieldsLoop
						}
					}
				}
			}
		}
	})

	b.Run("minio/simdjson-go-foreach", func(b *testing.B) {
		var reusedJSON simdjson.ParsedJson
		var rootArr simdjson.Array
		var elemArr simdjson.Array
		for i := 0; i < b.N; i++ {
			pj, err := simdjson.Parse(jsonUnmarshalBenchmarkData, &reusedJSON)
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			err = pj.ForEach(func(i simdjson.Iter) error {
				rootArr, err := i.Array(&rootArr)
				if err != nil {
					return err
				}
				elem := -1
				rootArr.ForEach(func(j simdjson.Iter) {
					elem++
					elemArr, err := j.Array(&elemArr)
					if err != nil {
						return
					}
					res = append(res, &Kline{})
					field := -1
					elemArr.ForEach(func(k simdjson.Iter) {
						field++
						switch field {
						case 0:
							intVal, err := k.Int()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].OpenTime = intVal
						case 1:
							strVal, err := k.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].Open = string(strVal)
						case 2:
							strVal, err := k.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].High = string(strVal)
						case 3:
							strVal, err := k.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].Low = string(strVal)
						case 4:
							strVal, err := k.StringBytes()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].Close = string(strVal)
						case 6:
							intVal, err := k.Int()
							if err != nil {
								b.Fatalf("invalid kline response")
							}
							res[elem].CloseTime = intVal
						}
					})
				})
				return nil
			})
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
		}
	})

	b.Run("bytedance/sonic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			temp := make([][]any, 1000)
			dec := sonic.ConfigDefault.NewDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData))
			dec.UseNumber()
			if err := dec.Decode(&temp); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, len(temp))
			for i, item := range temp {
				ot, err := item[0].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := item[6].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: ct,
				}
			}
		}
	})

	b.Run("bytedance/sonic-fastest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			temp := make([][]any, 1000)
			dec := sonic.ConfigFastest.NewDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData))
			dec.UseNumber()
			if err := dec.Decode(&temp); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, len(temp))
			for i, item := range temp {
				ot, err := item[0].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := item[6].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: ct,
				}
			}
		}
	})

	b.Run("bytedance/sonic-get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parsed, err := sonic.Get(jsonUnmarshalBenchmarkData)
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, 1000)
			for i := range res {
				elem := parsed.Index(i)
				ot, err := elem.Index(0).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				op, err := elem.Index(1).String()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				hi, err := elem.Index(2).String()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				lo, err := elem.Index(3).String()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				cl, err := elem.Index(4).String()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := elem.Index(6).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      op,
					High:      hi,
					Low:       lo,
					Close:     cl,
					CloseTime: ct,
				}
			}
		}
	})

	b.Run("bytedance/sonic-get-strict", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parsed, err := sonic.Get(jsonUnmarshalBenchmarkData)
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, 1000)
			for i := range res {
				elem := parsed.Index(i)
				ot, err := elem.Index(0).StrictInt64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				op, err := elem.Index(1).StrictString()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				hi, err := elem.Index(2).StrictString()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				lo, err := elem.Index(3).StrictString()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				cl, err := elem.Index(4).StrictString()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := elem.Index(6).StrictInt64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      op,
					High:      hi,
					Low:       lo,
					Close:     cl,
					CloseTime: ct,
				}
			}
		}
	})

	b.Run("tidwall/gjson", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parsed := gjson.ParseBytes(jsonUnmarshalBenchmarkData)
			arr := parsed.Array()
			res = make([]*Kline, len(arr))
			for i, item := range arr {
				res[i] = &Kline{
					OpenTime:  item.Get("0").Int(),
					Open:      item.Get("1").String(),
					High:      item.Get("2").String(),
					Low:       item.Get("3").String(),
					Close:     item.Get("4").String(),
					CloseTime: item.Get("6").Int(),
				}
			}
		}
	})

	b.Run("tidwall/gjson-2darray", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parsed := gjson.ParseBytes(jsonUnmarshalBenchmarkData)
			arr := parsed.Array()
			res = make([]*Kline, len(arr))
			for i, item := range arr {
				fields := item.Array()
				res[i] = &Kline{
					OpenTime:  fields[0].Int(),
					Open:      fields[1].String(),
					High:      fields[2].String(),
					Low:       fields[3].String(),
					Close:     fields[4].String(),
					CloseTime: fields[6].Int(),
				}
			}
		}
	})

	b.Run("tidwall/gjson-map", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parsed := gjson.ParseBytes(jsonUnmarshalBenchmarkData).Value()
			res = make([]*Kline, len(parsed.([]any)))
			for i, item := range parsed.([]any) {
				item := item.([]any)
				res[i] = &Kline{
					OpenTime:  int64(item[0].(float64)),
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: int64(item[6].(float64)),
				}
			}
		}
	})

	b.Run("francoispqt/goja", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dec := gojay.BorrowDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData))
			defer dec.Release()
			klines := make(Klines, 0, 1000)
			if err := dec.DecodeArray(&klines); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = klines
		}
	})

	b.Run("buger/jsonparser", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res = make([]*Kline, 0, 1000)
			_, err := jsonparser.ArrayEach(jsonUnmarshalBenchmarkData, func(elem []byte, _ jsonparser.ValueType, _ int, err error) {
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ot, err := jsonparser.GetInt(elem, "[0]")
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				op, err := jsonparser.GetString(elem, "[1]")
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				hi, err := jsonparser.GetString(elem, "[2]")
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				lo, err := jsonparser.GetString(elem, "[3]")
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				cl, err := jsonparser.GetString(elem, "[4]")
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := jsonparser.GetInt(elem, "[6]")
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res = append(res, &Kline{
					OpenTime:  ot,
					Open:      op,
					High:      hi,
					Low:       lo,
					Close:     cl,
					CloseTime: ct,
				})
			})
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
		}
	})

	b.Run("buger/jsonparser-arrayeach", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res = make([]*Kline, 0, 1000)
			_, err := jsonparser.ArrayEach(jsonUnmarshalBenchmarkData, func(elem []byte, _ jsonparser.ValueType, _ int, err error) {
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res = append(res, &Kline{})
				elemIdx := len(res) - 1
				fieldIdx := -1
				_, err = jsonparser.ArrayEach(elem, func(field []byte, _ jsonparser.ValueType, _ int, err error) {
					fieldIdx++
					if err != nil {
						b.Fatalf("failed to parse json: %v", err)
					}
					switch fieldIdx {
					case 0:
						ot, err := jsonparser.ParseInt(field)
						if err != nil {
							b.Fatalf("failed to parse json: %v", err)
						}
						res[elemIdx].OpenTime = ot
					case 1:
						res[elemIdx].Open = string(field)
					case 2:
						res[elemIdx].High = string(field)
					case 3:
						res[elemIdx].Low = string(field)
					case 4:
						res[elemIdx].Close = string(field)
					case 6:
						ct, err := jsonparser.ParseInt(field)
						if err != nil {
							b.Fatalf("failed to parse json: %v", err)
						}
						res[elemIdx].CloseTime = ct
					}
				})
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
			})
			if err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
		}
	})

	b.Run("json-iterator/go", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			temp := make([][]any, 1000)
			dec := jsoniter.NewDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData))
			dec.UseNumber()
			if err := dec.Decode(&temp); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, len(temp))
			for i, item := range temp {
				ot, err := item[0].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := item[6].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: ct,
				}
			}
		}
	})

	b.Run("json-iterator/go-fastest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			temp := make([][]any, 1000)
			dec := jsoniter.ConfigFastest.NewDecoder(bytes.NewReader(jsonUnmarshalBenchmarkData))
			dec.UseNumber()
			if err := dec.Decode(&temp); err != nil {
				b.Fatalf("failed to parse json: %v", err)
			}
			res = make([]*Kline, len(temp))
			for i, item := range temp {
				ot, err := item[0].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				ct, err := item[6].(json.Number).Int64()
				if err != nil {
					b.Fatalf("failed to parse json: %v", err)
				}
				res[i] = &Kline{
					OpenTime:  ot,
					Open:      item[1].(string),
					High:      item[2].(string),
					Low:       item[3].(string),
					Close:     item[4].(string),
					CloseTime: ct,
				}
			}
		}
	})

	_ = res
}
