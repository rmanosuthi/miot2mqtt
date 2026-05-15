package ha

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/device"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const MQTTClientId = "miot2mqtt"

var ErrMqttConnInit = errors.New("failed to initialize MQTT connection")
var ErrMqttConnSub = errors.New("failed to subscribe to MQTT topic")
var ErrHaDevInit = errors.New("failed to initialize Home Assistant device")

type HaConn struct {
	global *config.Global
	mqtt   mqtt.Client
	l      *slog.Logger
}

func NewConnection(
	ctx context.Context,
	logger *slog.Logger,
	cfg *config.Global,
	mqttDebugLogger *log.Logger,
	mqttErrorLogger *log.Logger,
) (HaConn, error) {
	ctx, cfConnLost := context.WithCancelCause(ctx)
	var res HaConn
	opts := mqtt.NewClientOptions().AddBroker(cfg.MQTT.Endpoint)
	opts.SetClientID(MQTTClientId)

	opts.SetUsername(cfg.MQTT.Username)
	opts.SetPassword(cfg.MQTT.Password)

	opts.SetConnectTimeout(time.Second)
	opts.SetKeepAlive(time.Duration(cfg.MQTT.KeepAliveSeconds) * time.Second)
	//opts.SetConnectionNotificationHandler(connNotification)
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		cfConnLost(err)
	})
	opts.SetAutoAckDisabled(true)

	client := mqtt.NewClient(opts)
	res = HaConn{
		global: cfg,
		l:      logger.With("component", "ha"),
		mqtt:   client,
	}
	return res, nil
}

type HaConsume struct {
	DeviceMap device.MapDevices
}

func (conn HaConn) Consume(ctx context.Context, cs HaConsume) error {
	var wg sync.WaitGroup
	c := conn.mqtt

	tk := c.Connect()
	tk.Wait()
	err := tk.Error()
	if err != nil {
		return errors.Join(ErrMqttConnInit, err)
	}

	for _, md := range cs.DeviceMap {
		dev, err := InitDevice(md)
		if err != nil {
			if errors.Is(err, errors.ErrUnsupported) {
				slog.Warn("device has no HA integration", "reason", err)
			} else {
				slog.Error("failed to initialize Home Assistant device", "reason", err)
			}
		} else {
			disc, err := dev.Discovery()
			if err != nil {
				slog.Warn("discovery payload fail", "reason", err)
				continue
			}
			tk := c.Publish(fmt.Sprintf("homeassistant/device/%v/config", dev.Ident()), 0, false, disc)
			tk.Wait()
			err = tk.Error()
			if err != nil {
				slog.Warn("discovery fail", "reason", err)
			}
			wg.Go(func() {
				err := dev.Subscribe(ctx, conn.l, c)
				if err != nil {
					slog.Error("device sub failed", "reason", err)
				}
			})
		}
	}

	wg.Wait()
	return nil
}
