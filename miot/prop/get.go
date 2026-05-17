package prop

type GetProp struct {
	Response ResponseEntry
	Error    error
}

type GetPropsReq = map[PropKey]*GetProp

// Puts keys in req into the wire format struct.
func MakeGetQuery(connId uint32, req GetPropsReq) (RawQuery, error) {
	var props []QueryEntry
	for key, _ := range req {
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
