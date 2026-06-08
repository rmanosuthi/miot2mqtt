package prop

import (
	"encoding/json"
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

// PropKey is a typesafe accessor for operations on
// device properties.
type PropKey struct {
	DID  string
	SIID config.SpecID
	PIID config.SpecID
	ty   wire.MiType
}

func Parse(spec *config.Spec) (map[PropKey]config.SpecProp, error) {
	diid := ""

	props := make(map[PropKey]config.SpecProp)
	for _, svc := range spec.Services {
		siid := svc.IID
		for _, prop := range svc.Properties {
			piid := prop.IID
			key := PropKey{
				DID:  diid,
				SIID: siid,
				PIID: piid,
				ty:   prop.Format,
			}
			props[key] = prop
		}
	}
	return props, nil
}

type KeyUnwrapError struct {
	ExpectedType wire.MiType
	Value        json.RawMessage
}

func (e *KeyUnwrapError) Error() string {
	typeName, _ := e.ExpectedType.MarshalText()
	return fmt.Sprintf("expected type %v, found %v", string(typeName), string(e.Value))
}

// Unwrap extracts a single response from a list of responses
// using a key and a value map.
// This function guarantees the response has already been
// typechecked against the key's type.
func (key *PropKey) Unwrap(resp []responseEntry, valueMap wire.ValueMap) (ResponseEntry, error) {
	kdid := key.DID
	ksiid := key.SIID
	kpiid := key.PIID
	for _, rprop := range resp {
		rdid := rprop.DID
		rsiid := rprop.SIID
		rpiid := rprop.PIID
		if kdid == rdid && ksiid == rsiid && kpiid == rpiid {
			if rprop.Code != 0 {
				return ResponseEntry{}, fmt.Errorf("response has error code %v", rprop.Code)
			}
			if rprop.Value == nil {
				return ResponseEntry{}, nil
			}
			miVal, err := key.ty.Convert(rprop.Value, valueMap.MiotHA)
			if err != nil {
				return ResponseEntry{}, &KeyUnwrapError{
					ExpectedType: key.ty,
					Value:        rprop.Value,
				}
			}
			return ResponseEntry{
				Code:  rprop.Code,
				Value: miVal,
			}, nil
		}
	}
	return ResponseEntry{}, fmt.Errorf("this key cannot unwrap this response")
}
