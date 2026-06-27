package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrDevEv = errors.New("incoming event")

// A Device in this package is its Home Assistant-facing representation.
//
// The non-threadsafe [miot.Device] is wrapped
// and external communication is done through
// [Device.Post] to avoid concurrency issues.
// Conversely, Device is a producer to [Device.Pool]
// which communicates with [DevicePool].
//
// Only one command is processed at a time.
//
// # Home Assistant integration
//
// A device makes its presence known to Home Assistant through a Discovery message.
// This message enumerates a device's type and its properties.
//
// Discovery message are sent when homeassistant/status becomes "online".
// The submission path for a device is:
//
//	homeassistant/device/{DeviceID}/config
//
// and is of the form [Discovery].
//
// A device's status is periodically updated over MQTT
// when [time.Ticker] ticks, calling [Device.Report].
//
// See [MQTTHandle] for more MQTT related info.
//
// # State
//
// miot2mqtt generally does not keep track of the device's state.
// The exception to this is power on/off, as
// certain properties need the device to be on before
// they can be changed.
//
// The result of periodic reporting by [Device.Report]
// as mentioned earlier is not stored, with exception of
// power on/off.
//
// A device must have at least one component with a [PropDecl]
// which handles power state.
// This is the property matched by URN name "on".
//
// When the device is marked as off and an
// incoming command is received,
// the device will first be turned on,
// then the command will be processed.
type Device struct {
	ticker        *time.Ticker
	components    []ComponentHandle
	md            miot.Device
	l             *slog.Logger
	rewrite       map[Topic]RewriteEntry
	commandTopics TopicMap
	stateTopics   TopicMap
	enumTopics    DpMqConnInfo
	mbox          chan any
	pool          chan<- any

	power      bool
	powerTopic PropertyTopic
}

type DeviceArgs struct {
	MiotDevice   miot.Device
	GlobalLogger *slog.Logger
	Pool         chan<- any
}

func NewDevice(ctx context.Context, args DeviceArgs) (Device, error) {
	md := &args.MiotDevice
	did := md.DeviceID
	cmps, err := MatchDevice(md)
	if err != nil {
		return Device{}, err
	}

	var l *slog.Logger
	l = args.GlobalLogger.WithGroup("device")
	if md.Alias == "" {
		l = l.With("did", did)
	} else {
		l = l.With("did", did, "alias", md.Alias)
	}
	deviceTopic := GetDeviceTopic(did)

	var components []ComponentHandle
	commandTopics := make(TopicMap)
	stateTopics := make(TopicMap)
	rewrite := make(map[Topic]RewriteEntry)

	cmpDst := AttachComponentDst{
		Rewrite: rewrite,
		// leave OnTopic empty
	}

	for _, cmp := range cmps {
		ch, err := AttachComponent(&cmpDst, AttachComponentArgs{
			Template:    cmp,
			MiotDevice:  md,
			DeviceTopic: deviceTopic,
		})
		if err != nil {
			if cmp.Mandatory {
				return Device{}, fmt.Errorf("component attach: %w", err)
			} else {
				l.Warn("attach component", "name", cmp.Alias, "reason", err)
				continue
			}
		}
		components = append(components, ch)
		maps.Insert(commandTopics, maps.All(ch.CommandTopics))
		maps.Insert(stateTopics, maps.All(ch.StateTopics))
	}

	mbox := make(chan any)

	resp := DpMqConnInfo{
		DID:        did,
		SubTopics:  commandTopics,
		DeviceMbox: mbox,
	}

	if !cmpDst.FoundOn {
		return Device{}, errors.New("device has no power property")
	}

	l.Debug("command", "topics", commandTopics)
	l.Debug("state", "topics", stateTopics)
	return Device{
		ticker:        time.NewTicker(time.Second * 30),
		components:    components,
		md:            *md,
		l:             l,
		commandTopics: commandTopics,
		stateTopics:   stateTopics,
		enumTopics:    resp,
		mbox:          mbox,
		pool:          args.Pool,
		rewrite:       rewrite,
		powerTopic:    cmpDst.OnTopic,
	}, nil
}

// SetPowerState sets the power state of the device to state.
// A DevMqPost containing an MQTT topic and a boolean payload
// is returned to be used for updating HA.
func (dev *Device) SetPowerState(ctx context.Context, state bool) (DevMqPost, error) {
	entry := dev.commandTopics[dev.powerTopic.Command(true)]
	key := entry.PropKey
	stateRaw, err := json.Marshal(state)
	if err != nil {
		return DevMqPost{}, err
	}
	sp, err := prop.NewSetProp(key, stateRaw, &wire.IdentityValueMap{})
	if err != nil {
		return DevMqPost{}, err
	}

	err = dev.md.SetProperty(ctx, key, &sp)
	if err != nil {
		return DevMqPost{}, err
	} else {
		dev.power = state
		return DevMqPost{
			DID: dev.md.DeviceID,
			Payload: map[Topic]json.RawMessage{
				dev.powerTopic.State(true): stateRaw,
			},
		}, nil
	}
}

func (dev *Device) Post(msg any) error {
	dev.mbox <- msg
	return nil
}

func (dev *Device) Declare(ctx context.Context) ([]byte, error) {
	info, err := dev.md.Info(ctx)
	if err != nil {
		return nil, err
	}
	disc, err := NewDiscovery(&dev.md, dev.components, &info)
	if err != nil {
		return nil, err
	}

	return json.Marshal(&disc)
}

// Subscribe starts the Device service.
//
// Shutdown steps:
//
//  1. Handle remaining mailbox messages
//  2. Update availability to offline
//  3. Return
func (dev *Device) Subscribe(ctx context.Context) error {
	l := dev.l
	did := dev.md.DeviceID
	l.Info("service is live")
	// make each component online
	cmpsOnline := make(map[Topic]json.RawMessage)
	for _, ch := range dev.components {
		cmpsOnline[ch.AvailTopic] = []byte("online")
	}
	dev.pool <- DevMqPost{
		DID:     did,
		Payload: cmpsOnline,
	}
	var run bool = true

	// report once on start
	ctxReport, cancelReport := context.WithTimeout(ctx, time.Second)
	defer cancelReport()
	report, err := dev.Report(ctxReport)
	if err != nil {
		l.Error("report", "reason", err)
	} else {
		err := json.Unmarshal(report.Payload[dev.powerTopic.State(true)], &dev.power)
		if err != nil {
			l.Error("get on state", "reason", err)
		}

		dev.pool <- report
	}

	for run {
		select {
		case <-dev.ticker.C:
			l.Debug("report")
			ctxReport, cancelReport := context.WithTimeout(ctx, time.Second)
			defer cancelReport()
			report, err := dev.Report(ctxReport)
			if err != nil {
				l.Error("report", "reason", err)
				continue
			}
			err = json.Unmarshal(report.Payload[dev.powerTopic.State(true)], &dev.power)
			if err != nil {
				l.Error("get on state", "reason", err)
			}

			dev.pool <- report
		case <-ctx.Done():
			run = false
		case msg, ok := <-dev.mbox:
			if !ok {
				l.Debug("mailbox closed")
				run = false
				continue
			}
			l.Debug("new message")
			dev.handleMboxMsg(ctx, msg)
		}
	}
	return dev.shutdown(ctx)
}

func (dev *Device) shutdown(ctx context.Context) error {
	l := dev.l.With("stage", "shutdown")
	did := dev.md.DeviceID

	// step 1
	l.Debug("drain mbox msgs")
	ctxDrain, cancelDrain := context.WithTimeout(context.Background(), time.Second*2)
	defer cancelDrain()
	for msg := range dev.mbox {
		err := dev.handleMboxMsg(ctxDrain, msg)
		if err != nil {
			l.Error("mailbox drain", "reason", err)
			continue
		}
	}

	// step 2
	l.Debug("update avail offline")
	post := DevMqPost{
		DID:     did,
		Payload: make(map[Topic]json.RawMessage),
	}
	for _, cmp := range dev.components {
		post.Payload[cmp.AvailTopic] = []byte("offline")
	}
	dev.pool <- post

	l.Info("done")
	return nil
}

func (dev *Device) RewritePublish(pub *MqDevPublish) error {
	// Most messages don't need to be rewritten.
	// Mutate pub in place to avoid pointless copies.
	entry, ok := dev.rewrite[pub.Topic]
	if !ok {
		// Exit early if no such rewrite entry exists.
		dev.l.Debug("no rewrite")
		return nil
	}

	if !slices.Equal(pub.Payload, entry.FromPayload) {
		// Topic match but payload mismatch.
		// Not an error;
		// example is rewrite fan speed == 0 to fan off
		// which ignores fan speed != 0
		dev.l.Debug("rewrite mismatch")
		return nil
	}

	// Topic and payload match.
	dev.l.Debug("rewrite", "src", pub.Topic, "dst", entry.ToTopic)
	pub.Topic = entry.ToTopic.Command(true)
	pub.Payload = entry.ToPayload
	return nil
}

func (dev *Device) handleMboxMsg(ctx context.Context, msg any) error {
	l := dev.l
	did := dev.md.DeviceID

	switch msg := msg.(type) {
	case MqDevPublish:
		l.Debug("publish")
		err := dev.RewritePublish(&msg)
		if err != nil {
			return err
		}

		// we may need to turn the device on first
		if !dev.power && msg.Topic != dev.powerTopic.Command(true) {
			l.Debug("powering on")
			newPower, err := dev.SetPowerState(ctx, true)
			if err != nil {
				return err
			}

			// update status
			dev.power = true
			dev.pool <- newPower
		}

		ctxSet, cancelSet := context.WithTimeout(ctx, time.Second)
		defer cancelSet()

		post, err := dev.handleSetProp(ctxSet, msg.Topic, msg.Payload)
		if err != nil {
			return errors.Join(ErrDevEv, err)
		}

		select {
		case dev.pool <- post:
			return nil
		default:
			return ErrChFull
		}
	case DpDevReqDiscovery:
		l.Debug("discovery")
		ctxDecl, cancelDecl := context.WithTimeout(ctx, time.Second)
		defer cancelDecl()
		decl, err := dev.Declare(ctxDecl)
		if err != nil {
			return err
		}
		l.Debug("created discovery payload", "msg", string(decl))

		discTopic := ResolveDiscovery(did)
		post := DevMqPost{
			DID: did,
			Payload: map[Topic]json.RawMessage{
				discTopic: decl,
			},
		}

		select {
		case dev.pool <- post:
			return nil
		default:
			return ErrChFull
		}
	default:
		return fmt.Errorf("unknown message: %v", msg)
	}
}

// GetDeviceTopic returns the base topic for a device.
func GetDeviceTopic(did wire.DeviceID) DeviceTopic {
	return DeviceTopic(BasePath + "/" + did.String())
}

// UniqueID calculates unique_id for a component.
func UniqueID(did wire.DeviceID, platform string, canon string) string {
	var sb strings.Builder

	sb.WriteString(BasePath)
	sb.WriteRune('_')
	sb.WriteString(did.String())
	sb.WriteRune('_')
	sb.WriteString(platform)
	sb.WriteRune('_')
	sb.WriteString(canon)

	return sb.String()
}

// A RewriteEntry is associated with a command topic to
// redirect a command to a different topic and
// change the payload's content.
//
// The marshaled form is [PropRewrite].
//
// Example: HA can send either "fan off" to ~/command
// or "fan speed 0" to ~/fan_speed/command, even when
// fan speed range has been defined as non-zero.
//
// miot may not support "fan speed 0" and will return an error.
// "fan speed 0" must then be rewritten to
// "fan off" and sent to ~/command instead.
//
// Topic rewrites are done before value rewrite,
// are restricted to a single component's scope,
// and are done by [Device.RewritePublish].
type RewriteEntry struct {
	FromPayload []byte
	ToTopic     PropertyTopic
	ToPayload   []byte
}
