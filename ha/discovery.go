// # Device Registration
//
// A device makes its presence known to Home Assistant through a Discovery message.
// This message enumerates a device's type and its properties.
//
// Discovery message are sent when homeassistant/status becomes "online".
// The submission path for a device is:
//
//	homeassistant/device/{DeviceID}/config
//
// and is of the form [Discovery].
//
// # Components
//
// A component is defined as [Component].
//
// Each component lives in:
//
//	miot2mqtt/{DeviceID}/{Component}
//
// and is defined as "~".
//
// Commands are submitted to:
//
//	~/{Property}/command
//
// and HA listens for state changes on:
//
//	~/{Property}/state
package ha

import (
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

// ComponentDiscovery is the marshaled form of [Component],
// which is a JSON map in a discovery message's map of components.
// Example, given a discovery message:
//
//	[...],
//	"cmps": {
//	  "some_unique_component_id1": {
//	    "p": "sensor",
//	    "device_class":"temperature",
//	    "unit_of_measurement":"°C",
//	    "value_template":"{{ value_json.temperature }}",
//	    "unique_id":"temp01ae_t"
//	  },
//	}
//
// The component canonicalized as "some_unique_component_id1"
// through [Canon] would
// have a ComponentDiscovery with keys
// "p", "device_class", "unit_of_measurement",
// "value_template", and "unique_id".
type ComponentDiscovery map[string]any

// Discovery is the message used for device registration.
type Discovery struct {
	Device DeviceInfo `json:"device"`
	Origin Origin     `json:"origin"`
	// Components must live in a JSON map.
	// The key does not seem to have any meaning,
	// so [ComponentHandle.Canon] is used in place.
	Components map[string]ComponentDiscovery `json:"components"`
}

// DeviceInfo lists the manufacturer info of a device.
type DeviceInfo struct {
	Identifiers     []string `json:"identifiers"`
	Alias           string   `json:"name"`
	Manufacturer    string   `json:"manufacturer"`
	Model           string   `json:"model"`
	SoftwareVersion string   `json:"sw_version"`
	HardwareVersion string   `json:"hw_version"`
	Serial          string   `json:"serial_number"`
}

func FromInfo(alias string, info *miot.Info) DeviceInfo {
	return DeviceInfo{
		Identifiers:     []string{info.DeviceID.String()},
		Alias:           alias,
		Manufacturer:    "Xiaomi",
		Model:           info.Model,
		SoftwareVersion: info.FirmwareVersion,
		HardwareVersion: info.HwVersion,
		Serial:          info.DeviceID.String(),
	}
}

// Origin lists miot2mqtt's info.
// All devices share the same payload.
type Origin struct {
	Name    string `json:"name"`
	Version string `json:"sw_version"`
	URL     string `json:"support_url"`
}

func NewOrigin() Origin {
	return Origin{
		Name:    "miot2mqtt",
		Version: "0.0.0",
		URL:     "https://github.com/rmanosuthi/miot2mqtt",
	}
}

// ResolveDiscovery returns the HA discovery topic for
// a device.
func ResolveDiscovery(did wire.DeviceID) Topic {
	return Topic("homeassistant/device/" + did.String() + "/config")
}

// NewDiscovery assembles a discovery message for a device by
// first looking up the device's info.
//
// This needs network access and mutates dev.
func NewDiscovery(dev *miot.Device, cmps []ComponentHandle, info *miot.Info) (Discovery, error) {
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
