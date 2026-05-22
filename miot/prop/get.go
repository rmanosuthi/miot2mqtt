package prop

type GetProp struct {
	Response ResponseEntry
	Error    error
}

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
