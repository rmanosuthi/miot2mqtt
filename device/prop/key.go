package prop

import (
	"fmt"
	"slices"

	"github.com/rmanosuthi/miot2mqtt/config"
)

type PropKey struct {
	DID  string
	SIID config.SpecID
	PIID config.SpecID
	Ref  config.SpecProp
}

func (key *PropKey) Read() bool {
	return slices.Contains(key.Ref.Access, "read")
}

func (key *PropKey) Write() bool {
	return slices.Contains(key.Ref.Access, "write")
}

func (key *PropKey) Notify() bool {
	return slices.Contains(key.Ref.Access, "notify")
}

func ParseFrom(spec *config.Spec) map[config.Urn]PropKey {
	// TODO
	diid := ""

	res := make(map[config.Urn]PropKey)
	for _, svc := range spec.Services {
		siid := svc.IID
		for _, prop := range svc.Properties {
			piid := prop.IID
			res[prop.Urn] = PropKey{
				DID:  diid,
				SIID: siid,
				PIID: piid,
				Ref:  prop,
			}
		}
	}
	return res
}

func (key *PropKey) Unwrap(resp []ResponseEntry) (ResponseEntry, error) {
	kdid := key.DID
	ksiid := key.SIID
	kpiid := key.PIID
	for _, rprop := range resp {
		rdid := rprop.DID
		rsiid := rprop.SIID
		rpiid := rprop.PIID
		if kdid == rdid && ksiid == rsiid && kpiid == rpiid {
			// TODO check type but don't make generics painful?
			return rprop, nil
		}
	}
	return ResponseEntry{}, fmt.Errorf("this key cannot unwrap this response")
}
