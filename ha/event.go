package ha

import (
	"github.com/rmanosuthi/miot2mqtt/config"
)

type Set struct {
	URN   config.URN `json:"urn"`
	Value any        `json:"value"`
}
