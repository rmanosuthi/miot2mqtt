package prop

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/config"
)

var ErrParseResponse = errors.New("failed to parse miot response")

type RawQuery struct {
	ID     uint32       `json:"id"`
	Method string       `json:"method"`
	Params []QueryEntry `json:"params"`
}

type QueryEntry struct {
	DID   string        `json:"did"`
	SIID  config.SpecID `json:"siid"`
	PIID  config.SpecID `json:"piid"`
	Value any           `json:"value,omitempty"`
}

type ResponseEntry struct {
	DID   string        `json:"did"`
	SIID  config.SpecID `json:"siid"`
	PIID  config.SpecID `json:"piid"`
	Code  int32         `json:"code"`
	Value any           `json:"value"`
}

type ResponseError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

func (e *ResponseError) Error() string {
	return fmt.Sprintf("code %v: %v", e.Code, e.Message)
}

type RawResponse struct {
	ID      uint32          `json:"id"`
	Error   *ResponseError  `json:"error,omitempty"`
	Result  []ResponseEntry `json:"result,omitempty"`
	ExeTime uint32          `json:"exe_time"`
}

// AllProperties is used as a predicate for GetProperties to return all properties.
//
// Not recommended as some devices can't respond to large queries.
func AllProperties(string, PropKey) bool {
	return true
}

func ParseResponse(jsonBytes []byte) ([]ResponseEntry, error) {
	var resp RawResponse
	err := json.Unmarshal(jsonBytes, &resp)
	if err != nil {
		return nil, errors.Join(ErrParseResponse, err)
	}
	if resp.Error != nil {
		return nil, errors.Join(ErrParseResponse, resp.Error)
	}
	return resp.Result, nil
}
