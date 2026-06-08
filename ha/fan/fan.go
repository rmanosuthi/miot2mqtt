package fan

import (
	"github.com/rmanosuthi/miot2mqtt/config"
	d "github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
)

type key = prop.PropKey
type spec = *config.SpecProp

var Fan = d.Component{
	Mandatory: true,
	Alias:     "Fan",
	Service:   "fan",
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
			Expand: func(s spec) (d.PropExpansion, error) {
				res, err := d.MinMax[uint8](s)
				if err != nil {
					return d.PropExpansion{}, err
				}
				return d.PropExpansion{
					Attributes: map[string]any{
						"speed_range_min": res.Min,
						"speed_range_max": res.Max,
					},
				}, nil
			},
		},
	},
}
