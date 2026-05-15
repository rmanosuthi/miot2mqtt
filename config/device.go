package config

import (
	"net/netip"
	"os"

	"github.com/BurntSushi/toml"
)

type Devices map[string]Device

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

// This is an unverified member of Devices so
// it doesn't implement defaultConfig
type Device struct {
	Alias   string
	Model   string
	IPAddr  netip.Addr
	Token   string
	Version uint64
	Enabled bool
}
