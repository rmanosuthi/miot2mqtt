// # Subscribe vs Route
//
// paho separates message delivery into two concepts.
// Networking analogy will be used:
//
//  1. Subscribe - listen on an address
//  2. Route - entry in a routing table
//
// MQTT statically subscribes to homeassistant/status
// and dynamically subscribes to each device's commandTopics by
// querying [DevicePool] through [MqDpConnected].
//
// A device's command topics are generated through [discovery.AttachComponent];
// see the *Topic structs in [discovery].
//
// Subscriptions and routes are set up when the connection is established
// through OnConnectionUp.
//
// Note: MQTT subscribes to specific non-wildcard topics on a [Device]'s behalf,
// but routing is done through a path glob
//
//	miot2mqtt/{DeviceID}/#
//
// We do not simply subscribe to a wildcard topic, else
// we will get a "loopback" message whenever a property's
// state is updated through us publishing it.
package ha

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/eclipse/paho.golang/autopaho"
	paho "github.com/eclipse/paho.golang/paho"
)

const MQTTClientId = "miot2mqtt"

var ErrMqttConnInit = errors.New("failed to initialize MQTT connection")
var ErrMqttConnSub = errors.New("failed to subscribe to MQTT topic")
var ErrHaDevInit = errors.New("failed to initialize Home Assistant device")

type MQTTArgs struct {
	BrokerURL url.URL
	Username  string
	Password  string
	Logger    *slog.Logger
	FromDp    <-chan any
	ToDp      chan<- any
}

type MQTTHandle struct {
	conn   *autopaho.ConnectionManager
	router *paho.StandardRouter
	fromDp <-chan any
	logger *slog.Logger
}

func NewMQTT(
	ctx context.Context,
	args MQTTArgs,
) (MQTTHandle, error) {
	l := args.Logger
	router := paho.NewStandardRouterWithDefault(func(p *paho.Publish) {
		// our routing handlers should handle everything
		l.Error("unroutable message", "topic", p.Topic, "payload", p.Payload)
	})
	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{
			&args.BrokerURL,
		},
		ConnectUsername:               args.Username,
		ConnectPassword:               []byte(args.Password),
		KeepAlive:                     30,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			go func() {
				router.RegisterHandler("homeassistant/status", func(pub *paho.Publish) {
					pl := string(pub.Payload)
					if pl == "online" {
						l.Debug("online")
						// let DevicePool know when HA becomes online
						args.ToDp <- MqDpReqDiscovery{
							Conn: cm,
						}
					}
				})
				_, err := cm.Subscribe(ctx, &paho.Subscribe{
					Subscriptions: []paho.SubscribeOptions{
						{Topic: "homeassistant/status", QoS: 1},
					},
				})
				if err != nil {
					l.Error("subscribe to HA status", "reason", err)
					return
				}

				// ask DevicePool for routes and subscriptions
				chSubs := make(chan DpMqConnInfo)
				args.ToDp <- MqDpConnected{
					ReplyTo: chSubs,
				}
				// collect subs here
				subs := make([]paho.SubscribeOptions, 0, 64)

			WaitForTopics:
				for {
					select {
					case <-ctx.Done():
						return
					case devTopics, ok := <-chSubs:
						if !ok {
							// DevicePool is done responding
							break WaitForTopics
						}
						for subTopic, _ := range devTopics.SubTopics {
							// append subs
							subs = append(subs, paho.SubscribeOptions{
								Topic: subTopic,
								QoS:   1,
							})
						}

						// setup route
						router.UnregisterHandler(devTopics.RouteGlob)
						router.RegisterHandler(devTopics.RouteGlob, devTopics.ForwardTo)
					}
				}

				// subscribe to collected topics
				_, err = cm.Subscribe(ctx, &paho.Subscribe{
					Subscriptions: subs,
				})
				if err != nil {
					l.Error("create subscription topics", "reason", err)
					return
				}

				l.Debug("created subscription topics", "count", len(subs))
			}()
		},
		OnConnectError: func(err error) {
			l.Warn("connection attempt failed", "reason", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: "miot2mqtt",
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					router.Route(pr.Packet.Packet())
					return true, nil
				},
			},
		},
	}

	conn, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		return MQTTHandle{}, err
	}

	return MQTTHandle{
		conn: conn, router: router,
		fromDp: args.FromDp,
		logger: l,
	}, nil
}

func (mq *MQTTHandle) Subscribe(ctx context.Context) error {
	l := mq.logger
	if err := mq.conn.AwaitConnection(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return mq.conn.Disconnect(context.TODO())
		case msg := <-mq.fromDp:
			switch msg := msg.(type) {
			case DevMqPost:
				for topic, payload := range msg.Payload {
					_, err := mq.conn.Publish(ctx, &paho.Publish{
						QoS:     1,
						Topic:   topic,
						Payload: payload,
					})
					if err != nil {
						l.Error("publish mq", "reason", err)
					}
				}
			default:
				return fmt.Errorf("unrecognized mq <- dp")
			}
		}
	}
}

func filterCommandTopics(dev *Device) []string {
	res := make([]string, 0, 8)
	for topic, _ := range dev.CommandTopics {
		res = append(res, topic)
	}
	return res
}
