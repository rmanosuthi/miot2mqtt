package fan

import d "github.com/rmanosuthi/miot2mqtt/ha/discovery"

var HorzAngle = d.Component{
	Mandatory: false,
	Alias:     "Horizontal Angle",
	Service:   "fan",
	Platform:  "number",
	Properties: d.PropDecls{
		"horizontal-angle": d.PropDecl{
			Mandatory: true,
			Prefix:    "",
			Expand: func(s spec) (d.PropExpansion, error) {
				res, err := d.MinMax[uint16](s)
				if err != nil {
					return d.PropExpansion{}, err
				}
				return d.PropExpansion{
					Attributes: map[string]any{
						"min": res.Min,
						"max": res.Max,
					},
				}, nil
			},
		},
	},
}
