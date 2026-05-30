package fan

import d "github.com/rmanosuthi/miot2mqtt/ha/discovery"

var HorzAngle = d.Component{
	Mandatory: false,
	Alias:     "Horizontal Angle",
	Platform:  "number",
	Properties: d.PropDecls{
		"horizontal-angle": d.PropDecl{
			Mandatory: true,
			Prefix:    "",
			More: func(s spec) (decl, error) {
				res, err := d.MinMax[uint16](&s)
				if err != nil {
					return decl{}, err
				}
				return decl{
					"min": res.Min,
					"max": res.Max,
				}, nil
			},
		},
	},
}
