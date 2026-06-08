package ha

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

type DevicePool struct {
	devs map[wire.DeviceID]Device
	// FIXME make unidirectional?
	fromDevs chan any
	mqSend   chan<- any
	mqRecv   <-chan any
	logger   *slog.Logger
}

type DevicePoolArgs struct {
	FromMQTT     <-chan any
	ToMQTT       chan<- any
	Resolver     *discovery.Resolver
	GlobalLogger *slog.Logger
}

func NewDevicePool(ctx context.Context, mDevs miot.MapDevices, args DevicePoolArgs) (DevicePool, error) {
	devs := make(map[wire.DeviceID]Device)
	chDevs := make(chan any)

	for did, md := range mDevs {
		dev, err := NewDevice(ctx, DeviceArgs{
			Resolver:     args.Resolver,
			MiotDevice:   md,
			GlobalLogger: args.GlobalLogger,
			Pool:         chDevs,
		})
		if err != nil {
			if _, ok := errors.AsType[ErrDevUnsupported](err); ok {
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
		logger:   args.GlobalLogger.WithGroup("pool"),
	}, nil
}

// Subscribe starts the DevicePool service.
//
// Shutdown steps:
//
//  1. Close device mailboxes
//  2. Cancel devices
//  3. Wait for device publish requests
//  4. Close mqSend to tell MQ we're done
//  5. Return
func (dp *DevicePool) Subscribe(ctx context.Context) error {
	var wg sync.WaitGroup
	mqSend := dp.mqSend
	mqRecv := dp.mqRecv
	l := dp.logger

	ctxDev, cancelDev := context.WithCancel(context.Background())
	defer cancelDev()

	for _, dev := range dp.devs {
		wg.Go(func() {
			dev.Subscribe(ctxDev)
		})
	}

	// keep draining messages from devs
	wg.Go(func() {
		var run bool = true
		for run {
			select {
			case <-ctx.Done():
				run = false
			case ev := <-dp.fromDevs:
				ctxDevEv, cancelEv := context.WithTimeout(ctx, time.Second)
				defer cancelEv()
				err := dp.handleFromDevs(ctxDevEv, ev)
				if err != nil {
					l.Error("receive from devices", "msg", ev, "reason", err)
					continue
				}
			}
		}
	})

	l.Info("service is live")
	var run bool = true
	for run {
		select {
		case <-ctx.Done():
			run = false
		case ev, ok := <-mqRecv:
			if !ok {
				run = false
				continue
			}
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
					go func() {
						// request device discovery
						err := dev.Post(DpDevReqDiscovery(ev))
						if err != nil {
							l.Error("request device discovery", "reason", err)
						}
						// device will reply later
					}()
				}
			default:
				l.Error("unknown mq event", "msg", ev)
				continue
			}
		}
	}
	return dp.shutdown(ctx, dpShutdownArgs{
		CancelDevs: cancelDev,
		WgDevs:     &wg,
		MQSend:     mqSend,
	})
}

type dpShutdownArgs struct {
	CancelDevs context.CancelFunc
	WgDevs     *sync.WaitGroup
	MQSend     chan<- any
}

func (dp *DevicePool) shutdown(ctx context.Context, args dpShutdownArgs) error {
	l := dp.logger.With("stage", "shutdown")

	// step 1
	l.Debug("close mboxes")
	for _, dev := range dp.devs {
		close(dev.mbox)
	}

	// step 2
	l.Debug("cancel devs")
	args.CancelDevs()

	// devs used to close this channel but
	// that led to panics.
	// close it here for now.
	go func() {
		args.WgDevs.Wait()
		close(dp.fromDevs)
	}()
	// step 3
	l.Debug("drain pub reqs")
	for ev := range dp.fromDevs {
		ctxEv, cancelEv := context.WithTimeout(context.Background(), time.Second)
		defer cancelEv()
		err := dp.handleFromDevs(ctxEv, ev)
		if err != nil {
			l.Error("msg from dev", "reason", err)
			continue
		}
	}

	// step 4
	l.Debug("close mqSend")
	close(args.MQSend)

	// step 5
	l.Info("done")
	return nil
}

func (dp *DevicePool) handleFromDevs(ctx context.Context, ev any) error {
	switch ev := ev.(type) {
	case DevMqPost:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dp.mqSend <- ev:
			return nil
		}
	default:
		return fmt.Errorf("unknown event from devices")
	}
}
