package ha

var Fan = []ComponentTemplate{
	// Fan
	{
		Mandatory: true,
		Alias:     "Fan",
		Service:   "fan",
		Platform:  "fan",
		Properties: PropDecls{
			"on": {
				Mandatory: true,
				Prefix:    "default",
			},
			"horizontal-swing": {
				Prefix: "oscillation",
			},
			"fan-level": {
				Prefix: "percentage",
				Rewrite: PropRewrite{
					Match:   []byte("0"),
					Target:  "default",
					Content: []byte("false"),
				},
				Expand: func(s spec) (PropExpansion, error) {
					res, err := MinMax[uint8](s)
					if err != nil {
						return PropExpansion{}, err
					}
					return PropExpansion{
						Attributes: map[string]any{
							"speed_range_min": res.Min,
							"speed_range_max": res.Max,
						},
					}, nil
				},
			},
		},
	},
	// Horizontal Angle
	{
		Mandatory: false,
		Alias:     "Horizontal Angle",
		Service:   "fan",
		Platform:  "number",
		Properties: PropDecls{
			"horizontal-angle": {
				Mandatory: true,
				Prefix:    "default",
				Expand: func(s spec) (PropExpansion, error) {
					res, err := MinMax[uint16](s)
					if err != nil {
						return PropExpansion{}, err
					}
					return PropExpansion{
						Attributes: map[string]any{
							"min": res.Min,
							"max": res.Max,
						},
					}, nil
				},
			},
		},
	},
}
