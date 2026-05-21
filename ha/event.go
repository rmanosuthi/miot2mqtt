package ha

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rmanosuthi/miot2mqtt/config"
)

type Event struct {
	Client  mqtt.Client
	Message mqtt.Message
}

type Set struct {
	URN   config.URN `json:"urn"`
	Value any        `json:"value"`
}
