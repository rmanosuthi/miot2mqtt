package prop

import (
	"encoding/json"
	"fmt"
	"iter"

	"github.com/rmanosuthi/miot2mqtt/config"
)

type SetProp struct {
	Response ResponseEntry
	Error    error
	value    json.RawMessage
}

type SetPropsReq = map[PropKey]*SetProp

func NewSetProp(spec config.SpecProp, value json.RawMessage) (SetProp, error) {
	_, ok := spec.Format.Cast(value)
	if !ok {
		return SetProp{}, fmt.Errorf("type mismatch")
	}

	return SetProp{
		value: value,
	}, nil
}

func MakeSetQuery(connId uint32, keys iter.Seq2[PropKey, *SetProp]) (rawQuery, error) {
	var props []queryEntry
	for key, setProp := range keys {
		props = append(props, queryEntry{
			DID:   key.DID,
			SIID:  key.SIID,
			PIID:  key.PIID,
			Value: setProp.value,
		})
	}
	query := rawQuery{
		ID:     connId,
		Method: "set_properties",
		Params: props,
	}
	return query, nil
}
