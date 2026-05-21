// # Home Assistant integration
//
// # TODO
//
// # Listening for commands
//
// Since [miot.Device] is not threadsafe,
// each [Device] listens on its components' paths on their behalf,
// avoiding any concurrency issues.
// This is implemented as [Device.CommandTopics],
// a lookup table mapping command topics to URNs.
//
// # Publishing state updates
//
// Conversely, [Device.StateTopics] is a lookup table mappin
// URNs to state topics.
package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/ha/fan"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const HintFan = "fan"

var ErrDevNoHint = errors.New("device has no class hint in spec")

type ErrDevUnsupported struct {
	did   wire.DeviceID
	model string
	cls   string
}

func (e ErrDevUnsupported) Error() string {
	return fmt.Sprintf("did %v model %v class %v", e.did, e.model, e.cls)
}

// A Device in this package is its Home Assistant-facing representation.
type Device struct {
	components    []discovery.ComponentHandle
	md            miot.Device
	l             *slog.Logger
	rsv           *discovery.Resolver
	CommandTopics map[string]config.URN
	StateTopics   map[config.URN]string
}

func NewDevice(rsv *discovery.Resolver, md miot.Device, logger *slog.Logger) (Device, error) {
	cmps, err := components(&md)
	if err != nil {
		return Device{}, err
	}
	l := logger.With("did", md.DeviceID, "alias", md.Alias, "model", md.Model)
	deviceTopic := rsv.GetDeviceTopic(md.DeviceID)

	var components []discovery.ComponentHandle
	commandTopics := make(map[string]config.URN)
	stateTopics := make(map[config.URN]string)

	for _, cmp := range cmps {
		ch, err := discovery.AttachComponent(cmp, &md, deviceTopic)
		if err != nil {
			if errors.Is(err, discovery.ErrNoMandatoryProp) {
				if !cmp.Mandatory() {
					continue
				} else {
					return Device{}, err
				}
			} else {
				return Device{}, err
			}
		}
		components = append(components, ch)
		maps.Insert(commandTopics, maps.All(ch.CommandTopics))
		maps.Insert(stateTopics, maps.All(ch.StateTopics))
	}

	l.Debug("command", "topics", commandTopics)
	l.Debug("state", "topics", stateTopics)
	return Device{
		components:    components,
		md:            md,
		l:             l,
		rsv:           rsv,
		CommandTopics: commandTopics,
		StateTopics:   stateTopics,
	}, nil
}

// components gets a [Component] group to attach to a device.
// All possible components a device may possess are returned.
func components(md *miot.Device) ([]discovery.Component, error) {
	hint, err := discovery.Hint(md)
	if err != nil {
		return nil, err
	}

	switch hint {
	case HintFan:
		return []discovery.Component{
			&fan.Fan{},
			&fan.HorzAngle{},
		}, nil
	default:
		return nil, ErrDevUnsupported{
			did:   md.DeviceID,
			model: md.Model,
			cls:   hint,
		}
	}
}

func (dev *Device) Declare(ctx context.Context, c mqtt.Client) ([]byte, error) {
	info, err := dev.md.Info(ctx)
	if err != nil {
		return nil, err
	}
	disc, err := dev.rsv.NewDiscovery(&dev.md, dev.components, &info)
	if err != nil {
		return nil, err
	}

	return json.Marshal(&disc)
}

// Subscribe spawns new goroutines in the [sync.WaitGroup] wg and listens on:
//
//  1. miot2mqtt/{DeviceID}/# - Get/Set requests.
//  2. homeassistant/status - Send discovery message.
//     Each device listens to this to avoid having to "stop the world".
//
// This function does not block.
func (dev Device) Subscribe(ctx context.Context,
	wg *sync.WaitGroup, c mqtt.Client, forceDiscov bool) {
	l := dev.l

	chStatus := make(chan Event)
	tkStat := c.Subscribe("homeassistant/status", 1, func(c mqtt.Client, m mqtt.Message) {
		select {
		case <-ctx.Done():
			close(chStatus)
		case chStatus <- Event{
			Client:  c,
			Message: m,
		}:
		default:
			l.Warn("dropping status message")
		}
	}).Wait()

	chEvent := make(chan Event)
	ft := filterCommandTopics(&dev)
	tkEv := c.SubscribeMultiple(ft, func(c mqtt.Client, m mqtt.Message) {
		select {
		case <-ctx.Done():
			close(chEvent)
		case chEvent <- Event{
			Client:  c,
			Message: m,
		}:
		default:
			l.Warn("dropping event")
		}
	}).Wait()

	var _ = tkStat
	var _ = tkEv

	wg.Go(func() {
		l.Info("device online")
		if forceDiscov {
			l.Warn("forcing discovery")
			err := dev.sendDiscovery(ctx, "online", c)
			if err != nil {
				l.Error("failed to force discovery", "reason", err)
			}
		}
		for {
			select {
			case <-ctx.Done():
				dev.l.Debug("shutting down")
				return
			case st := <-chStatus:
				l.Debug("new status")
				ctxSt, cancelSt := context.WithTimeout(ctx, time.Second)
				defer cancelSt()
				err := dev.handleStatus(ctxSt, st)
				if err != nil {
					l.Error("failed to handle status", "reason", err)
					continue
				}
			case ev := <-chEvent:
				topic := ev.Message.Topic()
				payload := ev.Message.Payload()
				var val any
				err := json.Unmarshal(payload, &val)
				if err != nil {
					l.Error("failed to unmarshal set request", "reason", err)
					continue
				}

				urn, ok := dev.CommandTopics[topic]
				if !ok {
					l.Error("command topic not found", "topic", topic)
					continue
				}

				key, ok := dev.md.PropKeys[urn]
				if !ok {
					l.Error("key not found", "urn", urn)
					continue
				}

				req := prop.SetProp{
					Value: val,
				}
				err = dev.md.SetProperty(ctx, key, &req)
				if err != nil {
					l.Error("set prop failed", "reason", err)
					continue
				}

				l.Debug("set property")
				ev.Message.Ack()

				c.Publish(string(dev.StateTopics[urn]), 1, false, payload).Wait()
			}
		}
	})
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

func (dev *Device) sendDiscovery(ctx context.Context, status string, c mqtt.Client) error {
	l := dev.l
	if status == "online" {
		l.Debug("HA became online")
		decl, err := dev.Declare(ctx, c)
		if err != nil {
			return err
		}
		l.Debug("created discovery payload", "msg", string(decl))

		discPath := dev.rsv.ResolveDiscovery(dev.md.DeviceID)
		tk := c.Publish(discPath, 1, true, decl)
		select {
		case <-tk.Done():
			if err := tk.Error(); err != nil {
				return err
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		return nil
	}
}
