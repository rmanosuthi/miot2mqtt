package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const FanCmpFan = "fan"

var ErrFanOn = errors.New("failed to turn on")
var ErrFanOff = errors.New("failed to turn off")
var ErrFanOscOn = errors.New("failed to turn oscillation on")
var ErrFanOscOff = errors.New("failed to turn oscillation off")
var ErrFanSpeed = errors.New("failed to set fan speed")
var ErrFanInit = errors.New("failed to initialize fan")

// A FanDevice is a concrete device which is a fan,
// implementing all methods from [Device].
//
// Functionalities:
//   - (required) On/Off
//   - (optional) Fan Speed
//   - (optional) Oscillate
type FanDevice struct {
	// [NewFanDevice] accepts a [device.MiotDevice] and
	// stores it in here.
	miot.Device
	// Capabilities.
	FanCaps
	// Base component.
	baseCmp fanComponentFan
}

func (dev *FanDevice) handleCmdOn(ctx context.Context, fc *fanComponentFan, ev Message, l *slog.Logger) error {
	pl := string(ev.Message.Payload())
	if pl == "ON" {
		// on
		req, err := prop.NewSetProp(dev.On, true)
		if err != nil {
			return errors.Join(ErrFanOn, err)
		}
		err = dev.SetProperty(ctx, req)
		if err != nil {
			return errors.Join(ErrFanOn, err)
		}
		ev.Client.Publish(fc.StateTopic, 0, true, pl)
	} else {
		// off
		req, err := prop.NewSetProp(dev.On, false)
		if err != nil {
			return errors.Join(ErrFanOff, err)
		}
		err = dev.SetProperty(ctx, req)
		if err != nil {
			return errors.Join(ErrFanOff, err)
		}
		ev.Client.Publish(fc.StateTopic, 0, false, pl)
	}
	return nil
}

func (dev *FanDevice) handleCmdOscillate(ctx context.Context, fc *fanComponentFan, ev Message, l *slog.Logger) error {
	l.Debug("osc")
	pl := string(ev.Message.Payload())
	if pl == "oscillate_on" {
		// on
		req, err := prop.NewSetProp(dev.Oscillate, true)
		if err != nil {
			return errors.Join(ErrFanOscOn, err)
		}
		err = dev.SetProperty(ctx, req)
		if err != nil {
			return errors.Join(ErrFanOscOn, err)
		}
		ev.Client.Publish(fc.OscillationStateTopic, 0, true, pl)
	} else {
		// off
		req, err := prop.NewSetProp(dev.Oscillate, false)
		if err != nil {
			return errors.Join(ErrFanOscOff, err)
		}
		err = dev.SetProperty(ctx, req)
		if err != nil {
			return errors.Join(ErrFanOscOff, err)
		}
		ev.Client.Publish(fc.OscillationStateTopic, 0, false, pl)
	}
	return nil
}

func (dev *FanDevice) handleCmdSpeed(ctx context.Context, fc *fanComponentFan, ev Message, l *slog.Logger) error {
	l.Debug("per")
	pl := string(ev.Message.Payload())
	fanSpeed, err := strconv.Atoi(pl)
	if err != nil {
		slog.Error("failed to parse per", "reason", err)
	}
	req, err := prop.NewSetProp(dev.Percentage, fanSpeed)
	if err != nil {
		return errors.Join(ErrFanSpeed, err)
	}
	err = dev.SetProperty(ctx, req)
	if err != nil {
		return errors.Join(ErrFanSpeed, err)
	}
	ev.Client.Publish(fc.PercentageStateTopic, 0, false, pl)
	return nil
}

func (dev *FanDevice) Subscribe(ctx context.Context, logger *slog.Logger, c mqtt.Client) error {
	l := logger.With("did", dev.DeviceID)
	cmp := dev.baseCmp

	chOnCmd := make(chan Message)
	chOscCmd := make(chan Message)
	chPerCmd := make(chan Message)

	c.Subscribe(cmp.CommandTopic, 0, pipeTo(chOnCmd)).Wait()
	c.Subscribe(cmp.OscillationCommandTopic, 0, pipeTo(chOscCmd)).Wait()
	c.Subscribe(cmp.PercentageCommandTopic, 0, pipeTo(chPerCmd)).Wait()
	for {
		select {
		case <-ctx.Done():
			l.Debug("done")
			return nil
		case ev := <-chOnCmd:
			ev.Message.Ack()
			l := l.With("command", "on")
			cmdCtx, cancelCmd := context.WithTimeout(ctx, time.Second)
			defer cancelCmd()
			if err := dev.handleCmdOn(cmdCtx, &cmp, ev, l); err != nil {
				l.Error("handler failed", "reason", err)
			}
		case ev := <-chOscCmd:
			ev.Message.Ack()
			l := l.With("command", "oscillate")
			cmdCtx, cancelCmd := context.WithTimeout(ctx, time.Second)
			defer cancelCmd()
			if err := dev.handleCmdOscillate(cmdCtx, &cmp, ev, l); err != nil {
				l.Error("handler failed", "reason", err)
			}
		case ev := <-chPerCmd:
			ev.Message.Ack()
			l := l.With("command", "percentage")
			cmdCtx, cancelCmd := context.WithTimeout(ctx, time.Second)
			defer cancelCmd()
			if err := dev.handleCmdSpeed(cmdCtx, &cmp, ev, l); err != nil {
				l.Error("handler failed", "reason", err)
			}
		}
	}
}

func (dev *FanDevice) Discovery() ([]byte, error) {
	var alias string
	if dev.Alias == "" {
		alias = strconv.Itoa(int(dev.DeviceID))
	} else {
		alias = dev.Alias
	}
	discov := fandiscov{
		Base: discovery.Base{
			Device: discovery.Device{
				Identifiers: discovery.Ident(dev.DeviceID),
				Name:        alias,
			},
			Origin: discovery.Origin{
				Name: discovery.BaseTopic,
			},
			Components: map[string]any{
				dev.baseCmp.UniqueId: dev.baseCmp,
			},
		},
	}

	return json.Marshal(&discov)
}

func (dev *FanDevice) Ident() wire.DeviceID {
	return dev.DeviceID
}

func NewFanDevice(md miot.Device) (*FanDevice, error) {
	caps, err := GetFanCaps(&md)
	if err != nil {
		return nil, err
	}

	baseCmp := fancmps(md.DeviceID, &caps)

	return &FanDevice{
		Device:  md,
		FanCaps: caps,
		baseCmp: baseCmp,
	}, nil
}

type fandiscov struct {
	discovery.Base
}

type fanComponentFan struct {
	discovery.BaseCmp
	CommandTopic            string `json:"command_topic"`
	StateTopic              string `json:"state_topic"`
	OscillationStateTopic   string `json:"oscillation_state_topic,omitempty"`
	OscillationCommandTopic string `json:"oscillation_command_topic,omitempty"`
	PercentageStateTopic    string `json:"percentage_state_topic,omitempty"`
	PercentageCommandTopic  string `json:"percentage_command_topic,omitempty"`
	// TODO what's empty
	SpeedRangeMin uint8 `json:"speed_range_min,omitempty"`
	SpeedRangeMax uint8 `json:"speed_range_max,omitempty"`
}

type FanCaps struct {
	BasePath        string
	On              *prop.PropKey
	Oscillate       *prop.PropKey
	Percentage      *prop.PropKey
	PercentageMin   uint8
	PercentageMax   uint8
	HorizontalAngle *prop.PropKey
	HorizontalMin   uint16
	HorizontalMax   uint16
	VerticalAngle   *prop.PropKey
	VerticalMin     uint8
	VerticalMax     uint8
}

func GetFanCaps(dev *miot.Device) (FanCaps, error) {
	var caps FanCaps

	for _, key := range dev.Props {
		switch key.Ref.Name() {
		case "on":
			caps.On = &key
		case "horizontal-swing":
			caps.Oscillate = &key
		case "fan-level":
			caps.Percentage = &key
			if len(key.Ref.ValueList) == 0 {
				return caps, ErrFanInit
			}
			var minVal uint8 = 255
			var maxVal uint8 = 0
			for val := range config.VList[uint8](&key.Ref) {
				if val < minVal {
					minVal = val
				} else if val > maxVal {
					maxVal = val
				}
			}
			caps.PercentageMin = minVal
			caps.PercentageMax = maxVal
		}
	}
	caps.BasePath = fmt.Sprintf("%v/%v", discovery.BaseTopic, dev.DeviceID)

	return caps, nil
}

func fancmps(did wire.DeviceID, caps *FanCaps) fanComponentFan {
	baseCmp := discovery.NewBaseCmp(did, "fan", "Fan")
	fc := fanComponentFan{
		BaseCmp:      baseCmp,
		CommandTopic: baseCmp.Topic(did, "on/set"),
		StateTopic:   baseCmp.Topic(did, "on/state"),
	}

	if caps.Oscillate != nil {
		fc.OscillationCommandTopic = baseCmp.Topic(did, "oscillation/set")
		fc.OscillationStateTopic = baseCmp.Topic(did, "oscillation/state")
	}

	if caps.Percentage != nil {
		fc.PercentageCommandTopic = baseCmp.Topic(did, "percentage/set")
		fc.PercentageStateTopic = baseCmp.Topic(did, "percentage/state")
		fc.SpeedRangeMin = caps.PercentageMin
		fc.SpeedRangeMax = caps.PercentageMax
	}

	return fc
}
