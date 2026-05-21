package discovery

import (
	"errors"

	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

const IdxClassHint = 2

var ErrDevNoHint = errors.New("device has no class hint")
var ErrNoMandatoryProp = errors.New("no such mandatory property with name")

type resolvArgs struct {
	BasePath string
	DeviceID wire.DeviceID
	URN      string
	Prefix   string
}

// Resolver converts templates into more concrete forms.
// It does not have any side effects and is immutable,
// so it is safe for concurrent use.
type Resolver struct {
	basePath string
}

func NewResolver() (Resolver, error) {
	return Resolver{
		basePath: BasePath,
	}, nil
}

func (r *Resolver) ResolveDiscovery(did wire.DeviceID) string {
	return "homeassistant/device/" + did.String() + "/config"
}

// NewDiscovery creates a discovery message for a device.
func (r *Resolver) NewDiscovery(dev *miot.Device, cmps []ComponentHandle, info *miot.Info) (Discovery, error) {
	device := FromInfo(dev.Alias, info)

	origin := NewOrigin()

	components := make(map[string]ComponentDiscovery)

	for _, cmp := range cmps {
		components[cmp.Canon()] = cmp.Discovery
	}

	return Discovery{
		Device:     device,
		Origin:     origin,
		Components: components,
	}, nil
}

// Hint tries to figure out a device's class.
// Examples: "fan", "air-purifier".
func Hint(md *miot.Device) (string, error) {
	svc, ok := md.Services[IdxClassHint]
	if !ok {
		return "", ErrDevNoHint
	}
	svcName := svc.Type.Name.Value()
	return svcName, nil
}

// DeviceTopic returns the base topic for a device.
// This does not include the wildcard character.
func (r *Resolver) DeviceTopic(did wire.DeviceID) string {
	return BasePath + "/" + did.String() + "/"
}
