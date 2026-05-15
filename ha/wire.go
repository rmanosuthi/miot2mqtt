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
	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha/discovery"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const BaseTopic = discovery.BaseTopic

func matchClassHint(svc config.SpecService) bool {
	return svc.IID == 2
}

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
