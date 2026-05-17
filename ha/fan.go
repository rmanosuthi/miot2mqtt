package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
var ErrNoHorzAngle = errors.New("fan has no horizontal angle")

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
	baseCmp      fanComponentFan
	horzAngleCmp *fanComponentHorzAngle
}

func (dev *FanDevice) handleCmdOn(ctx context.Context, fc *fanComponentFan, ev Message, l *slog.Logger) error {
	pl := string(ev.Message.Payload())
	if pl == "ON" {
		// on
		req, err := prop.NewSetProp(dev.Props[*dev.On], true)
		if err != nil {
			return errors.Join(ErrFanOn, err)
		}
		err = dev.SetProperty(ctx, *dev.On, req)
		if err != nil {
			return errors.Join(ErrFanOn, err)
		}
		ev.Client.Publish(fc.StateTopic, 0, true, pl)
	} else {
		// off
		req, err := prop.NewSetProp(dev.Props[*dev.On], false)
		if err != nil {
			return errors.Join(ErrFanOff, err)
		}
		err = dev.SetProperty(ctx, *dev.On, req)
		if err != nil {
			return errors.Join(ErrFanOff, err)
		}
		ev.Client.Publish(fc.StateTopic, 0, false, pl)
	}
	return nil
}

func (dev *FanDevice) handleCmdOscillate(ctx context.Context, fc *fanComponentFan, ev Message, l *slog.Logger) error {
	l.Debug("osc")
	key := *dev.Oscillate
	pl := string(ev.Message.Payload())
	if pl == "oscillate_on" {
		// on
		req, err := prop.NewSetProp(dev.Props[key], true)
		if err != nil {
			return errors.Join(ErrFanOscOn, err)
		}
		err = dev.SetProperty(ctx, key, req)
		if err != nil {
			return errors.Join(ErrFanOscOn, err)
		}
		ev.Client.Publish(fc.OscillationStateTopic, 0, true, pl)
	} else {
		// off
		req, err := prop.NewSetProp(dev.Props[key], false)
		if err != nil {
			return errors.Join(ErrFanOscOff, err)
		}
		err = dev.SetProperty(ctx, key, req)
		if err != nil {
			return errors.Join(ErrFanOscOff, err)
		}
		ev.Client.Publish(fc.OscillationStateTopic, 0, false, pl)
	}
	return nil
}

func (dev *FanDevice) handleCmdSpeed(ctx context.Context, fc *fanComponentFan, ev Message, l *slog.Logger) error {
	l.Debug("per")
	key := *dev.Percentage
	pl := string(ev.Message.Payload())
	fanSpeed, err := strconv.Atoi(pl)
	if err != nil {
		slog.Error("failed to parse per", "reason", err)
	}
	req, err := prop.NewSetProp(dev.Props[key], fanSpeed)
	if err != nil {
		return errors.Join(ErrFanSpeed, err)
	}
	err = dev.SetProperty(ctx, key, req)
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
	chHorzCmd := make(chan Message)

	c.Subscribe(cmp.CommandTopic, 0, pipeTo(chOnCmd)).Wait()
	c.Subscribe(cmp.OscillationCommandTopic, 0, pipeTo(chOscCmd)).Wait()
	c.Subscribe(cmp.PercentageCommandTopic, 0, pipeTo(chPerCmd)).Wait()

	if dev.horzAngleCmp != nil {
		c.Subscribe(dev.horzAngleCmp.CommandTopic, 0, pipeTo(chHorzCmd)).Wait()
	}
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
		case ev := <-chHorzCmd:
			ev.Message.Ack()
			s := string(ev.Message.Payload())
			l := l.With("command", "horizontal_angle")
			cmdCtx, cancelCmd := context.WithTimeout(ctx, time.Second)
			defer cancelCmd()
			iv, err := strconv.Atoi(s)
			if err != nil {
				l.Error("handler failed", "reason", err)
			}

			val := uint16(iv)
			if val > dev.FanCaps.HorizontalMax {
				l.Error("handler failed", "reason", "horizontal angle out of range")
			}

			key := *dev.FanCaps.HorizontalAngle
			req, err := prop.NewSetProp(dev.Props[key], val)
			if err != nil {
				l.Error("handler failed", "reason", err)
			}
			if err := dev.SetProperty(cmdCtx, key, req); err != nil {
				l.Error("handler failed", "reason", err)
			}
			ev.Client.Publish(dev.horzAngleCmp.StateTopic, 0, false, val)
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

	components := make(map[string]any)
	components[dev.baseCmp.UniqueId] = dev.baseCmp

	if dev.horzAngleCmp != nil {
		components[dev.horzAngleCmp.UniqueId] = *dev.horzAngleCmp
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
			Components: components,
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

	baseCmp := caps.getFanComponent(md.DeviceID)
	horzAngleCmp, _ := caps.getHorzAngleComponent(md.DeviceID)

	return &FanDevice{
		Device:       md,
		FanCaps:      caps,
		baseCmp:      baseCmp,
		horzAngleCmp: horzAngleCmp,
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

type fanComponentHorzAngle struct {
	discovery.BaseCmp
	CommandTopic string `json:"command_topic"`
	StateTopic   string `json:"state_topic"`
	Min          uint16 `json:"min"`
	Max          uint16 `json:"max"`
	Step         uint16 `json:"step"`
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
	VerticalSwing   *prop.PropKey
	VerticalMin     uint8
	VerticalMax     uint8
}

func GetFanCaps(dev *miot.Device) (FanCaps, error) {
	var caps FanCaps

	for key, prop := range dev.Props {
		switch prop.Name() {
		case "on":
			caps.On = &key
		case "horizontal-swing":
			caps.Oscillate = &key
		case "vertical-swing":
			caps.VerticalSwing = &key
		case "fan-level":
			caps.Percentage = &key
			if len(prop.ValueList) == 0 {
				return caps, ErrFanInit
			}
			var minVal uint8 = math.MaxUint8
			var maxVal uint8 = 0
			for val := range config.VList[uint8](&prop) {
				if val < minVal {
					minVal = val
				} else if val > maxVal {
					maxVal = val
				}
			}
			caps.PercentageMin = minVal
			caps.PercentageMax = maxVal
		case "horizontal-angle":
			if len(prop.ValueRange) < 2 {
				return caps, ErrFanInit
			}
			var minVal uint16 = math.MaxUint16
			var maxVal uint16 = 0
			for _, jsonVal := range prop.ValueRange {
				iv, err := jsonVal.Int64()
				if err != nil {
					return caps, ErrFanInit
				}

				val := uint16(iv)
				if val < minVal {
					minVal = val
				} else if val > maxVal {
					maxVal = val
				}
			}
			caps.HorizontalAngle = &key
			caps.HorizontalMin = minVal
			caps.HorizontalMax = maxVal
		case "vertical-angle":
			if len(prop.ValueRange) < 2 {
				return caps, ErrFanInit
			}
			var minVal uint8 = math.MaxUint8
			var maxVal uint8 = 0
			for _, jsonVal := range prop.ValueRange {
				iv, err := jsonVal.Int64()
				if err != nil {
					return caps, ErrFanInit
				}

				val := uint8(iv)
				if val < minVal {
					minVal = val
				} else if val > maxVal {
					maxVal = val
				}
			}
			caps.VerticalMin = minVal
			caps.VerticalMax = maxVal
		}
	}
	caps.BasePath = fmt.Sprintf("%v/%v", discovery.BaseTopic, dev.DeviceID)

	slog.Debug("fan caps", "did", dev.DeviceID, "caps", caps)
	return caps, nil
}

func (caps *FanCaps) getFanComponent(did wire.DeviceID) fanComponentFan {
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

func (caps *FanCaps) getHorzAngleComponent(did wire.DeviceID) (*fanComponentHorzAngle, error) {
	if caps.HorizontalMin == 0 && caps.HorizontalMax == 0 {
		return nil, ErrNoHorzAngle
	}

	baseCmp := discovery.NewBaseCmp(did, "number", "Horizontal Angle")
	cmp := fanComponentHorzAngle{
		BaseCmp:      baseCmp,
		CommandTopic: baseCmp.Topic(did, "set"),
		StateTopic:   baseCmp.Topic(did, "state"),
		Min:          caps.HorizontalMin,
		Max:          caps.HorizontalMax,
		Step:         1,
	}
	return &cmp, nil
}
