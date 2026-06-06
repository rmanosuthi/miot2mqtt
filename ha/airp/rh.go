package airp

import (
	d "github.com/rmanosuthi/miot2mqtt/ha/discovery"
)

var RelHumid = d.Component{
	Mandatory: false,
	Alias:     "Relative Humidity",
	Platform:  "sensor",
	Properties: d.PropDecls{
		"relative-humidity": d.PropDecl{
			Mandatory: true,
			Prefix:    "",
		},
	},
}
