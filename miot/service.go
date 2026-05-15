package miot

import "github.com/rmanosuthi/miot2mqtt/config"

type Services = map[config.SpecID]Service

type Service struct {
	IID         config.SpecID
	Type        config.URN
	Description string
}

func parseServices(spec *config.Spec) Services {
	res := make(Services)
	for _, svc := range spec.Services {
		res[svc.IID] = Service{
			IID:         svc.IID,
			Type:        svc.Type,
			Description: svc.Description,
		}
	}
	return res
}
