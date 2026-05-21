package fan

import d "github.com/rmanosuthi/miot2mqtt/ha/discovery"

var horzAngleDecl = d.PropDecls{
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
}

type HorzAngle struct{}

func (h *HorzAngle) Mandatory() bool {
	return false
}

func (h *HorzAngle) Alias() string {
	return "Horizontal Angle"
}

func (h *HorzAngle) Platform() string {
	return "number"
}

func (h *HorzAngle) Declare() d.PropDecls {
	return horzAngleDecl
}
