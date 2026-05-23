// # Device Concurrency
//
// Since [miot.Device] is not threadsafe,
// [Device] wraps that struct and
// only accepts external requests through its Mailbox.
//
// Note: Messages to DevicePool get sent to a common channel
// [Device.Pool]
// shared between all devices.
package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/ha/fan"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"

	paho "github.com/eclipse/paho.golang/paho"
)

const HintFan = "fan"

var ErrDevNoHint = errors.New("device has no class hint in spec")

type ErrDevUnsupported struct {
	did   wire.DeviceID
	model string
	cls   string
}

func (e ErrDevUnsupported) Error() string {
	return fmt.Sprintf("unsupported device: did %v model %v class %v", e.did, e.model, e.cls)
}

// A Device in this package is its Home Assistant-facing representation.
type Device struct {
	ticker        *time.Ticker
	components    []discovery.ComponentHandle
	md            miot.Device
	l             *slog.Logger
	rsv           *discovery.Resolver
	CommandTopics map[string]config.URN
	StateTopics   map[config.URN]string
	EnumTopics    DpMqConnInfo
	// Recognized: DpDevReqDiscovery
	Mailbox chan any
	Pool    chan<- any
}

type DeviceArgs struct {
	Resolver   *discovery.Resolver
	MiotDevice miot.Device
	Logger     *slog.Logger
	Pool       chan<- any
}

func NewDevice(ctx context.Context, args DeviceArgs) (Device, error) {
	md := &args.MiotDevice
	did := md.DeviceID
	cmps, err := components(md)
	if err != nil {
		return Device{}, err
	}
	l := args.Logger.With("did", did, "alias", md.Alias, "model", md.Model)
	deviceTopic := args.Resolver.GetDeviceTopic(did)

	var components []discovery.ComponentHandle
	commandTopics := make(map[string]config.URN)
	stateTopics := make(map[config.URN]string)

	for _, cmp := range cmps {
		ch, err := discovery.AttachComponent(cmp, md, deviceTopic)
		if err != nil {
			if errors.Is(err, discovery.ErrNoMandatoryProp) {
				if !cmp.Mandatory() {
					l.Debug("no optional component", "name", cmp.Alias())
					continue
				} else {
					return Device{}, fmt.Errorf("component attach: %w", err)
				}
			} else {
				return Device{}, fmt.Errorf("component attach: %w", err)
			}
		}
		components = append(components, ch)
		maps.Insert(commandTopics, maps.All(ch.CommandTopics))
		maps.Insert(stateTopics, maps.All(ch.StateTopics))
	}

	mbox := make(chan any)

	// TODO cmp Online
	resp := DpMqConnInfo{
		DID:       did,
		RouteGlob: deviceTopic.Glob(),
		SubTopics: commandTopics,
		ForwardTo: func(pub *paho.Publish) {
			mbox <- MqDevPublish{pub}
		},
	}

	l.Debug("command", "topics", commandTopics)
	l.Debug("state", "topics", stateTopics)
	return Device{
		ticker:        time.NewTicker(time.Second * 5),
		components:    components,
		md:            *md,
		l:             l,
		rsv:           args.Resolver,
		CommandTopics: commandTopics,
		StateTopics:   stateTopics,
		EnumTopics:    resp,
		Mailbox:       mbox,
		Pool:          args.Pool,
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

func (dev *Device) Declare(ctx context.Context) ([]byte, error) {
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

func (dev Device) Subscribe(ctx context.Context) {
	l := dev.l
	did := dev.md.DeviceID
	l.Info("device online")
	for {
		select {
		case <-dev.ticker.C:
			l.Debug("report")
			ctxReport, cancelReport := context.WithTimeout(ctx, time.Second)
			defer cancelReport()
			report, err := dev.Report(ctxReport)
			if err != nil {
				l.Error("failed to report", "reason", err)
				continue
			}

			dev.Pool <- report
		case <-ctx.Done():
			l.Debug("shutting down")
			post := DevMqPost{
				DID:     did,
				Payload: make(map[string][]byte),
			}
			for _, cmp := range dev.components {
				post.Payload[cmp.AvailTopic] = []byte("offline")
			}
			dev.Pool <- post
			return
		case msg := <-dev.Mailbox:
			l.Debug("new message")
			switch msg := msg.(type) {
			case MqDevPublish:
				pub := msg
				ctxEv, cancelEv := context.WithTimeout(ctx, time.Second)
				defer cancelEv()
				post, err := dev.handleEvent(ctxEv, pub.Topic, pub.Payload)
				if err != nil {
					l.Error("incoming event", "reason", err)
					continue
				}

				dev.Pool <- post
			case DpDevReqDiscovery:
				ctxDecl, cancelDecl := context.WithTimeout(ctx, time.Second)
				defer cancelDecl()
				decl, err := dev.Declare(ctxDecl)
				if err != nil {
					l.Error("query device info", "reason", err)
					continue
				}
				l.Debug("created discovery payload", "msg", string(decl))
				dev.Pool <- DevMqPost{
					DID: did,
					Payload: map[string][]byte{
						dev.rsv.ResolveDiscovery(did): decl,
					},
				}
			default:
				l.Error("unknown message")
			}
		}
	}
}
