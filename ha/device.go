package ha

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/rmanosuthi/miot2mqtt/device"
	"github.com/rmanosuthi/miot2mqtt/wire"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// A Device in this package is its Home Assistant-facing representation.
// There is no generic New() method; devices should have their own constructor.
//
// See [InitDevice] on where to call it from.
type Device interface {
	// Ident is its identifier used for creating topics.
	// This is currently a one-to-one mapping i.e.
	//   ID 12345678
	//   	== /miot2mqtt/12345678
	//   	== /homeassistant/device/12345678
	Ident() wire.DeviceID
	// Subscribe is intended to be a "main loop" for the device.
	// The device should attach to appropriate topics as necessary
	// and set up its own event loop which is assumed to block until shutdown.
	//
	// There is no Unsubscribe().
	//
	// The context is cancelled on program shutdown. Take care not to
	// block directly in mqtt's message handler.
	Subscribe(context.Context, *slog.Logger, mqtt.Client) error
	// Discovery is expected to generate a marshaled discovery message
	// that will be sent to Home Assistant.
	//
	// The message will be sent to:
	//   /homeassistant/device/{Device.Ident()}
	Discovery() ([]byte, error)
}

// InitDevice tries to figure out which device type a [MiotDevice] is by
// reading SIID == [matchClassHint].
//
// On success, a concrete device is initialized.
func InitDevice(md device.MiotDevice) (Device, error) {
	svcs := md.Spec.Services
	idx := slices.IndexFunc(svcs, matchClassHint)
	if idx == -1 {
		return nil, fmt.Errorf("failed to get service class")
	}
	switch svcs[idx].Name() {
	case "fan":
		return NewFanDevice(md)
	default:
		return nil, fmt.Errorf("unsupported device")
	}
}
