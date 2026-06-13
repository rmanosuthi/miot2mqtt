// # Supporting a new device type
//
// A device type has the general structure of:
//
//	Hint	string
//	Components	[]Component
//
// First, obtain its spec file:
//
//  1. Obtain its IP address and pairing token.
//
//  2. Skip if model name is already known: Get its info by running
//     ./cmd/utils -P {Prefix} -m dig -a {IPAddr} -t {TokenHex}
//
//  3. Determine the version to use.
//     Look for the highest-numbered value in
//     {Prefix}/vendor/miot_instances.json
//     with matching Model and Status == "released".
//
//  4. Fetch the spec file by running
//     ./cmd/utils -P {Prefix} -m spec -a {ModelName} -v {Version}
//
//  5. Spec file will be located in {Prefix}/vendor/spec/
//
// Hint should now be in the type field's URN.
// For example, this device's hint is "light":
//
//	{
//		"type": "urn:miot-spec-v2:device:light:[...]",
//		[...]
//	}
//
// For each service, determine its components (plural, may have multiple).
package ha

import (
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

// ErrDevUnsupported is returned when
// a device type is unrecognized by
// miot2mqtt.
type ErrDevUnsupported struct {
	did   wire.DeviceID
	model string
	cls   string
}

func (e ErrDevUnsupported) Error() string {
	return fmt.Sprintf("unsupported device: did %v model %v class %v", e.did, e.model, e.cls)
}

// MatchDevice gets a [Component] group to attach to a device by
// calling [Hint] to get the device type, then
// enumerating its components.
//
// Modify this function to add a new device type.
//
// All possible components a device may possess are returned.
func MatchDevice(md *miot.Device) ([]Component, error) {
	hint := md.Class
	switch hint {
	case "fan":
		return Fan, nil
	case "air-purifier":
		return AirPurifier, nil
	default:
		return nil, ErrDevUnsupported{
			did:   md.DeviceID,
			model: md.Model,
			cls:   hint,
		}
	}
}
