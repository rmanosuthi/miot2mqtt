package prop

import (
	"fmt"
	"iter"
	"reflect"

	"github.com/rmanosuthi/miot2mqtt/config"
)

type SetProp struct {
	Response ResponseEntry
	Error    error
	Value    any
}

type SetPropsReq = map[PropKey]SetProp

func NewSetProp[T any](spec config.SpecProp, value T) (SetProp, error) {
	expectedType := spec.Format
	foundType := reflect.TypeFor[T]()
	if foundType == nil {
		return SetProp{}, fmt.Errorf("set prop type nil")
	}
	if expectedType.ConvertibleTo(foundType) {
		return SetProp{
			Value: value,
		}, nil
	} else {
		return SetProp{}, fmt.Errorf("set prop type mismatch: expected %v, found %v", expectedType, foundType)
	}
}

func MakeSetQuery(connId uint32, keys iter.Seq2[PropKey, SetProp]) (RawQuery, error) {
	var props []QueryEntry
	for key, setProp := range keys {
		props = append(props, QueryEntry{
			DID:   key.DID,
			SIID:  key.SIID,
			PIID:  key.PIID,
			Value: setProp.Value,
		})
	}
	query := RawQuery{
		ID:     connId,
		Method: "set_properties",
		Params: props,
	}
	return query, nil
}
