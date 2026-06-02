package config

import (
	"net/netip"
	"os"

	"github.com/BurntSushi/toml"
)

// DeviceMeta is a pair of Device and Metaspec.
type DeviceMeta struct {
	Device Device
	Meta   Metaspec
}

// Devices is a map from string DID to Device.
type Devices map[string]Device

// DevicesMeta is a map from string DID to
// a pair of Device and Metaspec.
type DevicesMeta map[string]DeviceMeta

func (devs *Devices) Default(pfx *os.Root, gc *Global, hint *NoHint) error {
	res := make(Devices)
	*devs = res
	return nil
}

func (devs *Devices) Suffix(hint *NoHint) (string, error) {
	return "cache/devices.toml", nil
}

func (devs *Devices) MarshalFunc() ([]byte, error) {
	return toml.Marshal(devs)
}

func (devs *Devices) UnmarshalFunc(src []byte) error {
	return toml.Unmarshal(src, devs)
}

// Devices returns a map from string DID to Device,
// excluding Metaspec in the process.
func (dm *DevicesMeta) Devices() Devices {
	res := make(Devices)
	for did, d := range *dm {
		res[did] = d.Device
	}
	return res
}

// A Device is a section in the Devices config file,
// representing a single device.
//
// It must be paired with a DeviceID to be useful.
type Device struct {
	Alias   string
	Model   string
	IPAddr  netip.Addr
	Token   string
	Version uint64
	Enabled bool
}
