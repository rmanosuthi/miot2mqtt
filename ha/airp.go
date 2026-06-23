package ha

import "github.com/rmanosuthi/miot2mqtt/wire"

var AirPurifier = []ComponentTemplate{
	// Fan
	{
		Mandatory: true,
		Alias:     "Fan",
		Service:   "air-purifier",
		Platform:  "fan",
		Properties: PropDecls{
			"on": {
				Mandatory: true,
				Prefix:    "default",
			},
			"fan-level": {
				Prefix: "percentage",
				Expand: func(s spec) (PropExpansion, error) {
					res, err := MinMax[uint8](s)
					if err != nil {
						return PropExpansion{}, err
					}
					if res.Min == 0 {
						offsetMap := wire.IntOffsetMap[uint8](-1)
						// apply an offset
						return PropExpansion{
							Attributes: map[string]any{
								"speed_range_min": res.Min + 1,
								"speed_range_max": res.Max + 1,
							},
							ValueMap: &offsetMap,
						}, nil
					} else {
						return PropExpansion{
							Attributes: map[string]any{
								"speed_range_min": res.Min,
								"speed_range_max": res.Max,
							},
						}, nil
					}
				},
			},
		},
	},
	// Relative humidity
	{
		Mandatory: false,
		Alias:     "Relative Humidity",
		Service:   "environment",
		Platform:  "sensor",
		Properties: PropDecls{
			"relative-humidity": {
				Mandatory: true,
				Prefix:    "default",
			},
		},
	},
}
