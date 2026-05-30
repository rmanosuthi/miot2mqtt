package fan

import (
	"github.com/rmanosuthi/miot2mqtt/config"
	d "github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
)

type key = prop.PropKey
type spec = config.SpecProp
type decl = map[string]any

var Fan = d.Component{
	Mandatory: true,
	Alias:     "Fan",
	Platform:  "fan",
	Properties: d.PropDecls{
		"on": d.PropDecl{
			Mandatory: true,
			Prefix:    "",
		},
		"horizontal-swing": d.PropDecl{
			Prefix: "oscillation",
		},
		"fan-level": d.PropDecl{
			Prefix: "percentage",
			More: func(s spec) (decl, error) {
				res, err := d.MinMax[uint8](&s)
				if err != nil {
					return decl{}, err
				}
				return decl{
					"speed_range_min": res.Min,
					"speed_range_max": res.Max,
				}, nil
			},
		},
	},
}
