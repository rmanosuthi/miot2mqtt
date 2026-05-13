package prop

import (
	"github.com/rmanosuthi/miot2mqtt/config"
)

type GetProp struct {
	PropKey
	Response ResponseEntry
	Error    error
}

type GetPropsReq = map[config.Urn]GetProp

// Puts keys in req into the wire format struct.
func MakeGetQuery(connId uint32, req GetPropsReq) (RawQuery, error) {
	var props []QueryEntry
	for _, key := range req {
		props = append(props, QueryEntry{
			DID:   key.DID,
			SIID:  key.SIID,
			PIID:  key.PIID,
			Value: nil,
		})
	}
	query := RawQuery{
		ID:     connId,
		Method: "get_properties",
		Params: props,
	}
	return query, nil
}
