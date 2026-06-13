package ha

var Fan = []Component{
	// Fan
	{
		Mandatory: true,
		Alias:     "Fan",
		Service:   "fan",
		Platform:  "fan",
		Properties: PropDecls{
			"on": PropDecl{
				Mandatory: true,
				Prefix:    "",
			},
			"horizontal-swing": PropDecl{
				Prefix: "oscillation",
			},
			"fan-level": PropDecl{
				Prefix: "percentage",
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
			"horizontal-angle": PropDecl{
				Mandatory: true,
				Prefix:    "",
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
