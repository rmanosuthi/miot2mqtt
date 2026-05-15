package miot

import "github.com/rmanosuthi/miot2mqtt/config"

// TODO never tested!
type ActionKey struct {
	DIID string        `json:"diid"`
	SIID config.SpecID `json:"siid"`
	AIID config.SpecID `json:"aiid"`
}

type ActionKeys = map[config.URN]ActionKey
type Actions = map[ActionKey]config.SpecAction

func parseActions(spec *config.Spec) (ActionKeys, Actions) {
	diid := ""

	actionKeys := make(ActionKeys)
	actions := make(Actions)
	for _, svc := range spec.Services {
		siid := svc.IID
		for _, act := range svc.Actions {
			aurn := act.Type
			aiid := act.IID
			key := ActionKey{
				DIID: diid,
				SIID: siid,
				AIID: aiid,
			}
			actionKeys[aurn] = key
			actions[key] = act
		}
	}
	return actionKeys, actions
}
