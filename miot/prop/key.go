package prop

import (
	"encoding/json"
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/wire"
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

type KeyUnwrapError struct {
	FieldName    string
	ExpectedType wire.MiType
	Value        json.RawMessage
}

func (e *KeyUnwrapError) Error() string {
	typeName, _ := e.ExpectedType.MarshalText()
	return fmt.Sprintf("field %v, expected type %v, found %#v", e.FieldName, string(typeName), e.Value)
}

// Unwrap extracts a single response from a list of responses
// using a key and a spec.
// This function guarantees the response has already been
// typechecked against the spec's Format.
func (key *PropKey) Unwrap(spec config.SpecProp, resp []responseEntry) (ResponseEntry, error) {
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
			if _, ok := spec.Format.Cast(rprop.Value); !ok {
				return ResponseEntry{}, &KeyUnwrapError{
					FieldName:    spec.Name(),
					ExpectedType: spec.Format,
					Value:        rprop.Value,
				}
			}
			return ResponseEntry{
				Code:  rprop.Code,
				Value: rprop.Value,
			}, nil
		}
	}
	return ResponseEntry{}, fmt.Errorf("this key cannot unwrap this response")
}
