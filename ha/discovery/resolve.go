package discovery

import (
	"errors"

	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

const IdxClassHint = 2

var ErrDevNoHint = errors.New("device has no class hint")
var ErrNoMandatoryProp = errors.New("no such mandatory property with name")

// Resolver used to be an important struct in
// a private branch but has been hollowed out.
// More features may return.
type Resolver struct {
	basePath string
}

func NewResolver() (Resolver, error) {
	return Resolver{
		basePath: BasePath,
	}, nil
}

// ResolveDiscovery returns the HA discovery topic for
// a device.
func (r *Resolver) ResolveDiscovery(did wire.DeviceID) Topic {
	return Topic("homeassistant/device/" + did.String() + "/config")
}

// NewDiscovery assembles a discovery message for a device by
// first looking up the device's info.
//
// This needs network access and mutates dev.
func (r *Resolver) NewDiscovery(dev *miot.Device, cmps []ComponentHandle, info *miot.Info) (Discovery, error) {
	device := FromInfo(dev.Alias, info)

	origin := NewOrigin()

	components := make(map[string]ComponentDiscovery)

	for _, cmp := range cmps {
		components[cmp.Canon] = cmp.Discovery
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

// GetDeviceTopic returns the base topic for a device.
func (r *Resolver) GetDeviceTopic(did wire.DeviceID) DeviceTopic {
	return DeviceTopic(BasePath + "/" + did.String())
}
