package discovery

import (
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/wire"
)

const BaseTopic = "miot2mqtt"

type Device struct {
	Identifiers []string `json:"ids"`
	Name        string   `json:"name"`
}

type Origin struct {
	Name string `json:"name"`
}

// Base is a generic struct which devices may wrap to
// form a discovery payload.
type Base[C any] struct {
	Device     Device       `json:"dev"`
	Origin     Origin       `json:"o"`
	Components map[string]C `json:"cmps"`
}

// Base type for Component. Devices should wrap this.
// DeviceClass not provided here as some don't use it.
type BaseCmp struct {
	Platform string `json:"p"`
	UniqueId string `json:"uniq_id"`
}

func NewBaseCmp(did wire.DeviceID, cls string) BaseCmp {
	return BaseCmp{
		Platform: cls,
		UniqueId: fmt.Sprintf("%v_%v", did, cls),
	}
}

func Topic(did wire.DeviceID, suffix string) string {
	return fmt.Sprintf("%v/%v/%v", BaseTopic, did, suffix)
}

func Ident(did wire.DeviceID) []string {
	return []string{fmt.Sprintf("%v", did)}
}
