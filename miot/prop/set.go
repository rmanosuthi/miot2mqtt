package prop

import (
	"encoding/json"
	"iter"

	"github.com/rmanosuthi/miot2mqtt/wire"
)

type SetProp struct {
	Response ResponseEntry
	Error    error
	value    wire.MiValue
	valueMap wire.ValueMap
}

func (sp *SetProp) ValueMap(_ PropKey) wire.ValueMap {
	return sp.valueMap
}

// SetPropsReq is the request type for SetProperties.
// The value is a pointer since we mutate the argument in place.
type SetPropsReq = map[PropKey]*SetProp

func NewSetProp(key PropKey, value json.RawMessage, valueMap wire.ValueMap) (SetProp, error) {
	convVal, err := key.ty.Convert(value, valueMap.HAMiot)
	if err != nil {
		return SetProp{}, err
	}

	return SetProp{
		value:    convVal,
		valueMap: valueMap,
	}, nil
}

func MakeSetQuery(connId uint32, keys iter.Seq2[PropKey, *SetProp]) (rawQuery, error) {
	var props []queryEntry
	for key, setProp := range keys {
		props = append(props, queryEntry{
			DID:   key.DID,
			SIID:  key.SIID,
			PIID:  key.PIID,
			Value: setProp.value.RawMessage,
		})
	}
	query := rawQuery{
		ID:     connId,
		Method: "set_properties",
		Params: props,
	}
	return query, nil
}
