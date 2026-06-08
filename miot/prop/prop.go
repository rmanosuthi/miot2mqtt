package prop

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrParseResponse = errors.New("failed to parse miot response")

type rawQuery struct {
	ID     uint32       `json:"id"`
	Method string       `json:"method"`
	Params []queryEntry `json:"params"`
}

type queryEntry struct {
	DID   string          `json:"did"`
	SIID  config.SpecID   `json:"siid"`
	PIID  config.SpecID   `json:"piid"`
	Value json.RawMessage `json:"value,omitempty"`
}

// responseEntry is an opaque type for a device's response.
// The exported form is [ResponseEntry] and can be obtained through
// [PropKey.Unwrap].
type responseEntry struct {
	DID   string          `json:"did"`
	SIID  config.SpecID   `json:"siid"`
	PIID  config.SpecID   `json:"piid"`
	Code  int32           `json:"code"`
	Value json.RawMessage `json:"value"`
}

// ResponseEntry is an exported equivalent of [responseEntry]
// for a device's response.
// It can be obtained by [PropKey.Unwrap].
type ResponseEntry struct {
	Code  int32
	Value wire.MiValue
}

type responseError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

func (e *responseError) Error() string {
	return fmt.Sprintf("code %v: %v", e.Code, e.Message)
}

type rawResponse struct {
	ID      uint32          `json:"id"`
	Error   *responseError  `json:"error,omitempty"`
	Result  []responseEntry `json:"result,omitempty"`
	ExeTime uint32          `json:"exe_time"`
}

func ParseResponse(jsonBytes []byte) ([]responseEntry, error) {
	var resp rawResponse
	err := json.Unmarshal(jsonBytes, &resp)
	if err != nil {
		return nil, errors.Join(ErrParseResponse, err)
	}
	if resp.Error != nil {
		return nil, errors.Join(ErrParseResponse, resp.Error)
	}
	return resp.Result, nil
}

type Pair struct {
	Key  PropKey
	Spec config.SpecProp
}
