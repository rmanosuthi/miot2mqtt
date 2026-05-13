package prop

import (
	"errors"
	"iter"

	"github.com/rmanosuthi/miot2mqtt/config"
)

var ErrTypeMismatch = errors.New("type mismatch")

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
	res := SetProp{
		SetPropKey: SetPropKey{
			PropKey: key,
			Value:   value,
		},
	}
	// TODO check type
	return res, nil
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
