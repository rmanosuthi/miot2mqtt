package airp

import (
	"github.com/rmanosuthi/miot2mqtt/config"
	d "github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

type key = prop.PropKey
type spec = *config.SpecProp

var Fan = d.Component{
	Mandatory: true,
	Alias:     "Fan",
	Platform:  "fan",
	Properties: d.PropDecls{
		"on": d.PropDecl{
			Mandatory: true,
			Prefix:    "",
		},
		"fan-level": d.PropDecl{
			Prefix: "percentage",
			Expand: func(s spec) (d.PropExpansion, error) {
				res, err := d.MinMax[uint8](s)
				if err != nil {
					return d.PropExpansion{}, err
				}
				if res.Min == 0 {
					offsetMap := wire.IntOffsetMap[uint8](-1)
					// apply an offset
					return d.PropExpansion{
						Attributes: map[string]any{
							"speed_range_min": res.Min + 1,
							"speed_range_max": res.Max + 1,
						},
						ValueMap: &offsetMap,
					}, nil
				} else {
					return d.PropExpansion{
						Attributes: map[string]any{
							"speed_range_min": res.Min,
							"speed_range_max": res.Max,
						},
					}, nil
				}
			},
		},
	},
}
