package prop

import "github.com/rmanosuthi/miot2mqtt/wire"

type GetProp struct {
	Response ResponseEntry
	Error    error
	valueMap wire.ValueMap
}

func (gp *GetProp) ValueMap(_ PropKey) wire.ValueMap {
	return gp.valueMap
}

func NewGetProp(_ PropKey, vm wire.ValueMap) GetProp {
	return GetProp{valueMap: vm}
}

// GetPropsReq is the request type for GetProperties.
// The value is a pointer since we mutate the argument in place.
type GetPropsReq = map[PropKey]*GetProp

// Puts keys in req into the wire format struct.
func MakeGetQuery(connId uint32, req GetPropsReq) (rawQuery, error) {
	var props []queryEntry
	for key := range req {
		props = append(props, queryEntry{
			DID:   key.DID,
			SIID:  key.SIID,
			PIID:  key.PIID,
			Value: nil,
		})
	}
	query := rawQuery{
		ID:     connId,
		Method: "get_properties",
		Params: props,
	}
	return query, nil
}
