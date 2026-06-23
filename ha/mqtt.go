package ha

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	paho "github.com/eclipse/paho.golang/paho"
)

const MQTTClientId = "miot2mqtt"
const WaitMQTTUpSecs = 1

var ErrMqttConnInit = errors.New("failed to initialize MQTT connection")
var ErrMqttConnSub = errors.New("failed to subscribe to MQTT topic")
var ErrHaDevInit = errors.New("failed to initialize Home Assistant device")

type MQTTArgs struct {
	BrokerURL    url.URL
	Username     string
	Password     string
	GlobalLogger *slog.Logger
	FromDp       <-chan any
	ToDp         chan<- any
	CancelDp     context.CancelFunc
}

// MQTTHandle wraps the complex paho MQTT library and
// only exposes what we need.
// Its fields should be treated as implementation details.
//
// Message delivery involves these concepts summarized with
// networking terminology:
//
// # Listen
//
// MQTT statically subscribes to homeassistant/status
// and dynamically subscribes to each device's command topics.
//
// Subscriptions and routes are set up when the connection is established;
// see [mqttConnUp] and [DpMqConnInfo].
//
// # Route
//
// A router is required by paho.
// A standard one is used.
// MQTT subscribes to specific non-wildcard topics on a [Device]'s behalf,
// but routing is done through a path glob
//
//	miot2mqtt/{DeviceID}/#
//
// We do not simply subscribe to a wildcard topic, else
// we will get a "loopback" message whenever a property's
// state is updated through us publishing it.
//
// MQTT never talks to devices directly;
// communication is done through [DevicePool] by
// addressing a device by its DeviceID.
//
// # Proxy/rewrite
//
// Command and state types/values between HA and miot may not always match up.
// See [wire.ValueMap] for translating between the two.
//
// HA sometimes send values that should have gone to a different topic in
// a different form.
//
// Example: HA can send either "fan off" to ~/command
// or "fan speed 0" to ~/fan_speed/command, even when
// fan speed range has been defined as non-zero.
//
// miot may not support "fan speed 0" and will return an error.
// "fan speed 0" must then be rewritten to
// "fan off" and sent to ~/command instead.
//
// Topic rewrites are done before value rewrites.
// Topic rewrites are restricted to a single device's scope
// and are done by [Device.RewritePublish].
// However, cross-component references are possible.
type MQTTHandle struct {
	conn           *autopaho.ConnectionManager
	router         *paho.StandardRouter
	fromDp         <-chan any
	toDp           chan<- any
	logger         *slog.Logger
	chNoMoreIntake chan struct{}
	cancelDp       context.CancelFunc
	chConnUp       chan struct{}
}

type connUpArgs struct {
	ConnMan *autopaho.ConnectionManager
	Router  paho.Router
	Logger  *slog.Logger
	ToDP    chan<- any
}

func mqttConnUp(ctx context.Context, args connUpArgs) error {
	cm := args.ConnMan
	router := args.Router
	l := args.Logger
	toDp := args.ToDP
	l.Debug("starting conn up")

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
		return err
	}
	l.Debug("setup HA status sub")

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
			return ctx.Err()
		case devConnInfo, ok := <-chSubs:
			if !ok {
				// DevicePool is done responding
				break WaitForTopics
			}

			for topic := range devConnInfo.SubTopics {
				mqTopic := topic.Unwrap()

				// build handler
				handleCmd := func(pub *paho.Publish) {
					select {
					case devConnInfo.DeviceMbox <- MqDevPublish{
						Topic:   topic,
						Payload: pub.Payload,
					}:
					default:
						l.Error("device blocked, dropping message", "did", devConnInfo.DID)
					}
				}

				// append subs
				subs = append(subs, paho.SubscribeOptions{
					Topic: mqTopic,
					QoS:   1,
				})

				router.RegisterHandler(mqTopic, handleCmd)
			}
		}
	}

	// subscribe to collected topics
	_, err = cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: subs,
	})
	if err != nil {
		return err
	}
	l.Debug("setup subs")
	return nil
}

func NewMQTT(
	ctx context.Context,
	args MQTTArgs,
) (MQTTHandle, error) {
	l := args.GlobalLogger.WithGroup("mqtt")
	router := paho.NewStandardRouterWithDefault(func(p *paho.Publish) {
		// our routing handlers should handle everything
		l.Error("unroutable message", "topic", p.Topic, "payload", p.Payload)
	})

	chConnUp := make(chan struct{})
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
		OnConnectionDown: func() bool {
			l.Warn("connection down")
			return true
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			go func() {
				select {
				case _, _ = <-chNoMoreIntake:
					l.Debug("no more intake")
					close(chConnUp)
				default:
					chConnUp <- struct{}{}
				}
			}()
		},
		OnConnectError: func(err error) {
			l.Warn("connection attempt failed", "reason", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: "miot2mqtt",
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					l.Debug("new publish")
					router.Route(pr.Packet.Packet())
					return true, nil
				},
			},
		},
	}

	// paho stores the context and handing it ctx
	// would close the connection on cancellation.
	//
	// We want to do cleanup on shutdown so
	// just pass it a background context.
	conn, err := autopaho.NewConnection(context.Background(), cfg)
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
		chConnUp:       chConnUp,
	}, nil
}

// Subscribe starts the MQTT service.
func (mq *MQTTHandle) Subscribe(ctx context.Context) error {
	l := mq.logger
	l.Info("mq service is live")
	var run bool = true

	ctxWaitConn, cancelConn := context.WithTimeout(ctx, time.Second*WaitMQTTUpSecs)
	defer cancelConn()
	if err := mq.conn.AwaitConnection(ctxWaitConn); err != nil {
		l.Error("connect to MQTT", "reason", err)
		// DevicePool expects MQTT to cancel its context
		// skip to DP shutdown
		mq.shutdownDp(context.TODO())
		return err
	}
	l.Debug("mq conn live")

	var wg sync.WaitGroup
	if run {
		// force discovery
		wg.Go(func() {
			mq.toDp <- MqDpReqDiscovery{
				Conn: mq.conn,
			}
		})

		// It is important that fromDp is constantly drained
		// or DevicePool will block.
		// Run in its own goroutine.
		wg.Go(func() {
			var run bool = true
			for run {
				select {
				case <-ctx.Done():
					run = false
				case msg := <-mq.fromDp:
					ctxMsg, cancelMsg := context.WithTimeout(ctx, time.Second)
					defer cancelMsg()
					mqttHandleDpMsg(ctxMsg, mq.conn, msg)
				}
			}
		})
	}

	for run {
		select {
		case <-mq.chConnUp:
			l.Debug("mqtt loop: conn up")
			err := mqttConnUp(ctx, connUpArgs{
				ConnMan: mq.conn,
				Router:  mq.router,
				Logger:  mq.logger,
				ToDP:    mq.toDp,
			})
			if err != nil {
				l.Error("setup MQTT connection", "reason", err)
			}
		case <-ctx.Done():
			run = false
		}
	}
	wg.Wait()
	ctxShutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return mq.shutdown(ctxShutdown)
}

// shutdown stops the MQTT service. Its steps are:
//
//  1. Tell paho to not repopulate routes and subscriptions upon reconnect
//  2. Query DevicePool for latest routes and subs
//  3. Use the result to remove routes and subs
//  4. Close toDp to signal there will be no more received messages
//  5. Cancel DevicePool so it can publish remaining messages
//  6. Wait until that's done through fromDp getting closed
//  7. Disconnect and return
func (mq *MQTTHandle) shutdown(ctx context.Context) error {
	l := mq.logger.With("stage", "shutdown")

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
	//
	// need to gather all topics first
	l.Debug("remove subs routes")
	for devConnInfo := range chSubs {
		for mqTopic := range devConnInfo.SubTopics {
			topic := mqTopic.Unwrap()
			topics = append(topics, topic)
		}
	}
	// unsubscribe
	_, err := mq.conn.Unsubscribe(ctx, &paho.Unsubscribe{
		Topics: topics,
	})
	if err != nil {
		l.Error("unsubscribe from topics", "reason", err)
	}
	// remove routes
	for _, topic := range topics {
		mq.router.UnregisterHandler(topic)
	}

	// steps 4-6
	ctxStopDp, cancelDp := context.WithTimeout(context.Background(), time.Second)
	defer cancelDp()
	mq.shutdownDp(ctxStopDp)

	// step 7
	l.Debug("disconnect")
	ctxDisc, cancelDisc := context.WithTimeout(context.Background(), time.Second)
	defer cancelDisc()
	err = mq.conn.Disconnect(ctxDisc)
	if err != nil {
		l.Error("disconnect from MQTT", "reason", err)
	}

	l.Info("done")
	return nil
}

func (mq *MQTTHandle) shutdownDp(ctx context.Context) {
	l := mq.logger
	// step 4
	l.Debug("close toDp")
	close(mq.toDp)

	// step 5
	l.Debug("cancel dp")
	mq.cancelDp()

	var wg sync.WaitGroup
	// step 6
	l.Debug("wait fromDp")
	for msg := range mq.fromDp {
		wg.Go(func() {
			err := mqttHandleDpMsg(ctx, mq.conn, msg)
			if err != nil {
				l.Error("handle remaining message from device pool", "reason", err)
			}
		})
	}
	wg.Wait()
}

// mqttHandleDpMsg handles messages from DevicePool.
// This only contains "Publish to MQTT" operation for now.
func mqttHandleDpMsg(ctx context.Context, conn *autopaho.ConnectionManager, msg any) error {
	switch msg := msg.(type) {
	case DevMqPost:
		for topic, payload := range msg.Payload {
			_, err := conn.Publish(ctx, &paho.Publish{
				QoS:     1,
				Topic:   topic.Unwrap(),
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
