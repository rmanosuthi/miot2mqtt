package ha

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

type DevicePool struct {
	devs     map[wire.DeviceID]Device
	fromDevs <-chan any
	mqSend   chan<- any
	mqRecv   <-chan any
	logger   *slog.Logger
}

type DevicePoolArgs struct {
	FromMQTT <-chan any
	ToMQTT   chan<- any
	Resolver *discovery.Resolver
	Logger   *slog.Logger
}

func NewDevicePool(ctx context.Context, mDevs miot.MapDevices, args DevicePoolArgs) (DevicePool, error) {
	devs := make(map[wire.DeviceID]Device)
	chDevs := make(chan any)

	for did, md := range mDevs {
		dev, err := NewDevice(ctx, DeviceArgs{
			Resolver:   args.Resolver,
			MiotDevice: md,
			Logger:     args.Logger,
			Pool:       chDevs,
		})
		if err != nil {
			if err, ok := errors.AsType[ErrDevUnsupported](err); ok {
				continue
			} else {
				return DevicePool{}, fmt.Errorf("new device: %w", err)
			}
		}

		devs[did] = dev
	}

	return DevicePool{
		devs:     devs,
		fromDevs: chDevs,
		mqSend:   args.ToMQTT,
		mqRecv:   args.FromMQTT,
		logger:   args.Logger,
	}, nil
}

func (dp *DevicePool) Subscribe(ctx context.Context, wg *sync.WaitGroup) error {
	mqSend := dp.mqSend
	mqRecv := dp.mqRecv
	l := dp.logger

	for _, dev := range dp.devs {
		wg.Go(func() {
			dev.Subscribe(ctx)
		})
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-mqRecv:
			switch ev := ev.(type) {
			case MqDpConnected:
				l.Debug("from mq: connected")
				reply := ev.ReplyTo
				// enumerate and send
				for _, dev := range dp.devs {
					reply <- dev.EnumTopics
				}
				close(reply)
			case MqDpReqDiscovery:
				l.Debug("from mq: discovery")
				for _, dev := range dp.devs {
					// request device discovery
					dev.Mailbox <- DpDevReqDiscovery(ev)
					// device will reply later
					//dev.sendDiscovery(context.TODO(), "online", c)
				}
			default:
				return fmt.Errorf("unknown event from mq")
			}
		case ev := <-dp.fromDevs:
			l.Debug("from devs")
			switch ev := ev.(type) {
			case DevMqPost:
				mqSend <- ev
			default:
				return fmt.Errorf("unknown event from devices")
			}
		}
	}
}
