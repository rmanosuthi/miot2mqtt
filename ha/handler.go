package ha

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
)

// Report queries all registered properties of a Device.
func (dev *Device) Report(ctx context.Context) (DevMqPost, error) {
	props := make(map[string][]byte)
	keys := make(map[prop.PropKey]string)
	propResps, err := dev.md.GetProperties(ctx, func(urn config.URN, key prop.PropKey) bool {
		topic, ok := dev.StateTopics[urn]
		if ok {
			keys[key] = topic
		}
		return ok
	})
	if err != nil {
		return DevMqPost{}, err
	}

	for key, req := range propResps {
		topic := keys[key]
		encVal, err := json.Marshal(req.Response.Value)
		if err != nil {
			return DevMqPost{}, err
		}
		props[topic] = encVal
	}

	return DevMqPost{
		DID:     dev.md.DeviceID,
		Payload: props,
	}, nil
}

func (dev *Device) handleEvent(ctx context.Context, topic string, payload []byte) (DevMqPost, error) {
	var val json.RawMessage = json.RawMessage(payload)

	urn, ok := dev.CommandTopics[topic]
	if !ok {
		return DevMqPost{}, fmt.Errorf("command topic not found: %v", topic)
	}

	key, ok := dev.md.PropKeys[urn]
	if !ok {
		return DevMqPost{}, fmt.Errorf("key not found: %v", urn)
	}

	spec, ok := dev.md.Props[key]
	if !ok {
		return DevMqPost{}, fmt.Errorf("prop not found: %v", urn)
	}

	req, err := prop.NewSetProp(spec, val)
	if err != nil {
		return DevMqPost{}, fmt.Errorf("new set prop failed: %w", err)
	}

	err = dev.md.SetProperty(ctx, key, &req)
	if err != nil {
		return DevMqPost{}, fmt.Errorf("set prop failed: %w", err)
	}

	return DevMqPost{
		DID: dev.md.DeviceID,
		Payload: map[string][]byte{
			dev.StateTopics[urn]: payload,
		},
	}, nil
}
