package device

import "github.com/rmanosuthi/miot2mqtt/config"

type ActionKey struct {
	DIID string
	SIID config.SpecID
	AIID config.SpecID
	Ref  config.SpecAction
}

// parseActions returns a map of [ActionKey]s for use by [MiotDevice].
func parseActions(spec *config.Spec) map[config.URN]ActionKey {
	diid := ""

	res := make(map[config.URN]ActionKey)
	for _, svc := range spec.Services {
		siid := svc.IID
		for _, act := range svc.Actions {
			aiid := act.IID
			res[act.Type] = ActionKey{
				DIID: diid,
				SIID: siid,
				AIID: aiid,
				Ref:  act,
			}
		}
	}
	return res
}
