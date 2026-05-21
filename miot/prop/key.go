package prop

import (
	"fmt"
	"reflect"

	"github.com/rmanosuthi/miot2mqtt/config"
)

type PropKey struct {
	DID  string
	SIID config.SpecID
	PIID config.SpecID
}

func Parse(spec *config.Spec) (PropKeys, Props) {
	diid := ""

	propKeys := make(PropKeys)
	props := make(Props)
	for _, svc := range spec.Services {
		siid := svc.IID
		for _, prop := range svc.Properties {
			purn := prop.Urn
			piid := prop.IID
			key := PropKey{
				DID:  diid,
				SIID: siid,
				PIID: piid,
			}
			propKeys[purn] = key
			props[key] = prop
		}
	}
	return propKeys, props
}

func (key *PropKey) Unwrap(spec config.SpecProp, resp []ResponseEntry) (ResponseEntry, error) {
	kdid := key.DID
	ksiid := key.SIID
	kpiid := key.PIID
	for _, rprop := range resp {
		rdid := rprop.DID
		rsiid := rprop.SIID
		rpiid := rprop.PIID
		if kdid == rdid && ksiid == rsiid && kpiid == rpiid {
			expectedType := spec.Format
			foundType := reflect.TypeOf(rprop.Value)
			if foundType == nil {
				return rprop, nil
			}
			if expectedType.ConvertibleTo(foundType) {
				return rprop, nil
			} else {
				return rprop, fmt.Errorf("key unwrap type mismatch: expected %v, found %v", expectedType.Name(), foundType.Name())
			}
		}
	}
	return ResponseEntry{}, fmt.Errorf("this key cannot unwrap this response")
}
