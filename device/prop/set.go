package prop

import (
	"fmt"
	"iter"
	"reflect"

	"github.com/rmanosuthi/miot2mqtt/config"
)

type SetProp struct {
	SetPropKey
	Response ResponseEntry
	Error    error
}

type SetPropsReq = map[config.Urn]SetProp

type SetPropKey struct {
	*PropKey
	Value any
}

func NewSetProp[T any](key *PropKey, value T) (SetProp, error) {
	expectedType := key.Ref.Format
	foundType := reflect.TypeFor[T]()
	if foundType == nil {
		return SetProp{}, fmt.Errorf("set prop type nil")
	}
	if expectedType.ConvertibleTo(foundType) {
		return SetProp{
			SetPropKey: SetPropKey{
				PropKey: key,
				Value:   value,
			},
		}, nil
	} else {
		return SetProp{}, fmt.Errorf("set prop type mismatch: expected %v, found %v", expectedType, foundType)
	}
}

func MakeSetQuery(connId uint32, keys iter.Seq[SetProp]) (RawQuery, error) {
	var props []QueryEntry
	for key := range keys {
		props = append(props, QueryEntry{
			DID:   key.DID,
			SIID:  key.SIID,
			PIID:  key.PIID,
			Value: key.Value,
		})
	}
	query := RawQuery{
		ID:     connId,
		Method: "set_properties",
		Params: props,
	}
	return query, nil
}
