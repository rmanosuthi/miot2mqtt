package device

import "github.com/rmanosuthi/miot2mqtt/device/prop"

// GetCaps traverses a [MiotDevice]'s [prop.PropKey]s
// allowing the callback to use it for whatever it wants.
//
// See [ha.Device] for an example usage.
func GetCaps(cb func(propKey prop.PropKey) error, dev *MiotDevice) error {
	for _, prop := range dev.Props {
		if err := cb(prop); err != nil {
			return err
		}
	}
	return nil
}
