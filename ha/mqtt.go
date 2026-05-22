package ha

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot"

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
	DeviceMap miot.MapDevices
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

	rsv, _ := discovery.NewResolver()

	for _, md := range cs.DeviceMap {
		dev, err := NewDevice(ctx, DeviceArgs{
			Resolver:   &rsv,
			MiotDevice: md,
			Logger:     slog.Default(),
			MQTTClient: c,
		})
		if err != nil {
			if err, ok := errors.AsType[ErrDevUnsupported](err); ok {
				slog.Warn("device has no HA integration", "device", err)
				continue
			} else {
				return err
			}
		}

		dev.Subscribe(ctx, &wg, c, conn.global.MQTT.ForceDiscovery)
	}

	wg.Wait()
	return nil
}

func filterCommandTopics(dev *Device) map[string]byte {
	res := make(map[string]byte)
	for topic, _ := range dev.CommandTopics {
		res[string(topic)] = 1
	}
	return res
}
