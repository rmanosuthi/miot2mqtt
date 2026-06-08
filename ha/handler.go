package ha

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
)

// Report queries all registered properties of a Device.
func (dev *Device) Report(ctx context.Context) (DevMqPost, error) {
	props := make(map[discovery.Topic]json.RawMessage)
	req := make(prop.GetPropsReq)
	topics := make(map[prop.PropKey]discovery.Topic)

	// prepare request
	for topic, entry := range dev.StateTopics {
		key := entry.PropKey
		gp := prop.NewGetProp(key, entry.ValueMap)
		req[key] = &gp
		topics[key] = topic
	}

	err := dev.md.GetProperties(ctx, req)
	if err != nil {
		return DevMqPost{}, err
	}

	for key, req := range req {
		topic := topics[key]
		props[topic] = req.Response.Value.RawMessage
	}

	return DevMqPost{
		DID:     dev.md.DeviceID,
		Payload: props,
	}, nil
}

func (dev *Device) handleSetProp(ctx context.Context, rawTopic string, payload []byte) (DevMqPost, error) {
	var val json.RawMessage = json.RawMessage(payload)

	topic := discovery.Topic(rawTopic)
	cmdEntry, ok := dev.CommandTopics[topic]
	if !ok {
		return DevMqPost{}, fmt.Errorf("command topic not found: %v", topic)
	}

	key := cmdEntry.PropKey
	var stateTopic discovery.Topic
	for topic, entry := range dev.StateTopics {
		if entry.PropKey == key {
			stateTopic = topic
		}
	}
	if stateTopic == "" {
		return DevMqPost{}, fmt.Errorf("state topic not found: %v", topic)
	}

	req, err := prop.NewSetProp(key, val, cmdEntry.ValueMap)
	if err != nil {
		return DevMqPost{}, fmt.Errorf("new set prop failed: %w", err)
	}

	err = dev.md.SetProperty(ctx, key, req)
	if err != nil {
		return DevMqPost{}, fmt.Errorf("set prop failed: %w", err)
	}

	statePayload := map[discovery.Topic]json.RawMessage{
		stateTopic: payload,
	}

	return DevMqPost{
		DID:     dev.md.DeviceID,
		Payload: statePayload,
	}, nil
}
