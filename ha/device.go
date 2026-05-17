// # Device Registration
//
// A device makes its presence known to Home Assistant through a Discovery message.
// This message enumerates a device's type and its properties.
//
// Discovery messages are sent upon device initialization
// on every program start to:
//
//	homeassistant/device/{Device.Ident()}/config
//
// See [discovery] for more info.
package ha

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var ErrDevNoHint = errors.New("device has no class hint in spec")

const IdxClassHint = 2

type Message struct {
	Client  mqtt.Client
	Message mqtt.Message
}

func pipeTo(ch chan Message) mqtt.MessageHandler {
	return func(c mqtt.Client, m mqtt.Message) {
		select {
		case ch <- Message{
			Client:  c,
			Message: m,
		}:
		default:
		}
	}
}

type ErrDevUnsupported struct {
	did   wire.DeviceID
	model string
	cls   string
}

func (e ErrDevUnsupported) Error() string {
	return fmt.Sprintf("did %v model %v class %v", e.did, e.model, e.cls)
}

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
func InitDevice(md miot.Device) (Device, error) {
	svc, ok := md.Services[IdxClassHint]
	if !ok {
		return nil, ErrDevNoHint
	}
	svcName := svc.Type.Name.Value()
	switch svcName {
	case "fan":
		return NewFanDevice(md)
	default:
		return nil, errors.Join(errors.ErrUnsupported, ErrDevUnsupported{
			did:   md.DeviceID,
			model: md.Model,
			cls:   svcName,
		})
	}
}
