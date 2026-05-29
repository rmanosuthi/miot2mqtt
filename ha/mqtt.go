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
	"time"

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
	CancelDp  context.CancelFunc
}

type MQTTHandle struct {
	conn           *autopaho.ConnectionManager
	router         *paho.StandardRouter
	fromDp         <-chan any
	toDp           chan<- any
	logger         *slog.Logger
	chNoMoreIntake chan struct{}
	cancelDp       context.CancelFunc
}

type connUpArgs struct {
	ConnMan *autopaho.ConnectionManager
	Router  paho.Router
	Logger  *slog.Logger
	ToDP    chan<- any
}

func mqttConnUp(ctx context.Context, args connUpArgs) {
	cm := args.ConnMan
	router := args.Router
	l := args.Logger
	toDp := args.ToDP

	router.RegisterHandler("homeassistant/status", func(pub *paho.Publish) {
		pl := string(pub.Payload)
		if pl == "online" {
			l.Debug("online")
			// let DevicePool know when HA becomes online
			toDp <- MqDpReqDiscovery{
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
	toDp <- MqDpConnected{
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

	chNoMoreIntake := make(chan struct{})
	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{
			&args.BrokerURL,
		},
		ConnectUsername:               args.Username,
		ConnectPassword:               []byte(args.Password),
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         60,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			select {
			case _, _ = <-chNoMoreIntake:
			default:
				go func() {
					ctxConnUp, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					mqttConnUp(ctxConnUp, connUpArgs{
						ConnMan: cm,
						Router:  router,
						Logger:  l,
						ToDP:    args.ToDp,
					})
				}()
			}
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
		fromDp:         args.FromDp,
		toDp:           args.ToDp,
		logger:         l,
		chNoMoreIntake: chNoMoreIntake,
		cancelDp:       args.CancelDp,
	}, nil
}

// Subscribe starts the MQTT service.
//
// Shutdown steps:
//
//  1. Tell paho to not repopulate routes and subscriptions upon reconnect
//  2. Query DevicePool for latest routes and subs
//  3. Use the result to remove routes and subs
//  4. Close toDp to signal there will be no more received messages
//  5. Cancel DevicePool so it can publish remaining messages
//  6. Wait until that's done through fromDp getting closed
//  7. Disconnect and return
func (mq *MQTTHandle) Subscribe(ctx context.Context) error {
	l := mq.logger
	if err := mq.conn.AwaitConnection(ctx); err != nil {
		return err
	}

	go func() {
		mq.toDp <- MqDpReqDiscovery{
			Conn: mq.conn,
		}
	}()
	for {
		select {
		case <-ctx.Done():
			l := l.With("stage", "shutdown")
			// step 1
			l.Debug("no more intake")
			close(mq.chNoMoreIntake)

			// step 2
			l.Debug("query routes subs")
			chSubs := make(chan DpMqConnInfo)
			mq.toDp <- MqDpConnected{
				ReplyTo: chSubs,
			}
			topics := make([]string, 0, 64)

			// step 3
			// FIXME this might not 100% match what was set up initially
			// keep a copy around?
			l.Debug("remove routes subs")
			for devTopics := range chSubs {
				for topic, _ := range devTopics.SubTopics {
					topics = append(topics, topic)
				}
				mq.router.UnregisterHandler(devTopics.RouteGlob)
			}
			_, err := mq.conn.Unsubscribe(context.TODO(), &paho.Unsubscribe{
				Topics: topics,
			})
			// TODO err
			var _ = err

			// step 4
			l.Debug("close toDp")
			close(mq.toDp)

			// step 5
			l.Debug("cancel dp")
			mq.cancelDp()

			// step 6
			l.Debug("wait fromDp")
			for msg := range mq.fromDp {
				mqttHandleDpMsg(context.TODO(), mq.conn, msg)
			}

			// step 7
			l.Debug("disconnect")
			err = mq.conn.Disconnect(context.TODO())
			// TODO err

			l.Info("done")
			return nil

		case msg := <-mq.fromDp:
			mqttHandleDpMsg(ctx, mq.conn, msg)
		}
	}
}

func mqttHandleDpMsg(ctx context.Context, conn *autopaho.ConnectionManager, msg any) error {
	switch msg := msg.(type) {
	case DevMqPost:
		for topic, payload := range msg.Payload {
			_, err := conn.Publish(ctx, &paho.Publish{
				QoS:     1,
				Topic:   topic,
				Payload: payload,
			})
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unrecognized mq <- dp")
	}
}
