package ha

import (
	"context"
	"encoding/json"
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
)

func (dev *Device) Report(ctx context.Context, c mqtt.Client) error {
	keys := make(map[prop.PropKey]string)
	res, err := dev.md.GetProperties(ctx, func(urn config.URN, key prop.PropKey) bool {
		topic, ok := dev.StateTopics[urn]
		if ok {
			keys[key] = topic
		}
		return ok
	})
	if err != nil {
		return err
	}

	for key, req := range res {
		topic := keys[key]
		encVal, err := json.Marshal(req.Response.Value)
		if err != nil {
			return err
		}
		c.Publish(topic, 1, false, encVal)
	}
	return nil
}

func (dev *Device) handleEvent(ctx context.Context, ev Event) error {
	topic := ev.Message.Topic()
	payload := ev.Message.Payload()
	var val json.RawMessage = json.RawMessage(payload)

	urn, ok := dev.CommandTopics[topic]
	if !ok {
		return fmt.Errorf("command topic not found: %v", topic)
	}

	key, ok := dev.md.PropKeys[urn]
	if !ok {
		return fmt.Errorf("key not found: %v", urn)
	}

	spec, ok := dev.md.Props[key]
	if !ok {
		return fmt.Errorf("prop not found: %v", urn)
	}

	req, err := prop.NewSetProp(spec, val)
	if err != nil {
		return fmt.Errorf("new set prop failed: %w", err)
	}

	err = dev.md.SetProperty(ctx, key, &req)
	if err != nil {
		return fmt.Errorf("set prop failed: %w", err)
	}

	ev.Message.Ack()

	tk := ev.Client.Publish(string(dev.StateTopics[urn]), 1, false, payload)
	select {
	case <-tk.Done():
		return tk.Error()
	case <-ctx.Done():
		return nil
	}
}

func (dev *Device) handleStatus(ctx context.Context, st Event) error {
	status := string(st.Message.Payload())
	client := st.Client

	err := dev.sendDiscovery(ctx, status, client)
	if err != nil {
		return err
	}
	st.Message.Ack()
	return nil
}
