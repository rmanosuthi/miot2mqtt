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
)

// ComponentDiscovery is the marshaled form of [Component].
type ComponentDiscovery map[string]any

// Discovery is the message used for device registration
// created by [Resolver.NewDiscovery].
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
