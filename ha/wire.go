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
// with the payload being a struct which wraps [DiscovBase].
package ha

import (
	"github.com/rmanosuthi/miot2mqtt/config"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type dpd struct {
	Identifiers []string `json:"ids"`
	Name        string   `json:"name"`
}

type o struct {
	Name string `json:"name"`
}

type cm struct {
	Platform    string `json:"p"`
	DeviceClass string `json:"dev_cla"`
	UniqueId    string `json:"uniq_id"`
}

// DiscovBase is a generic struct which devices may wrap to
// form a discovery payload.
type DiscovBase[C any] struct {
	Device     dpd          `json:"dev"`
	Origin     o            `json:"o"`
	Components map[string]C `json:"cmps"`
}

// Base type for Component.
type CmpBase struct {
	Platform string `json:"p"`
	UniqueId string `json:"uniq_id"`
}

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
