package main

import (
	"context"
	"flag"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha"
	"github.com/rmanosuthi/miot2mqtt/ha/discovery"
	"github.com/rmanosuthi/miot2mqtt/miot"
)

func main() {
	var pfxPath, mode, inputFile, addDevices string
	var verbose, save bool
	flag.StringVar(&pfxPath, "P", "", "path to prefix")
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.StringVar(&mode, "m", "", "operation mode")
	flag.StringVar(&inputFile, "f", "", "input file")
	flag.BoolVar(&save, "s", false, "save state")
	flag.StringVar(&addDevices, "a", "", "new device info (format: ipaddr1,token1,ipaddr2,token2,...)")
	flag.Parse()

	var logLevel slog.Level
	if verbose {
		logLevel = slog.LevelDebug
	} else {
		logLevel = slog.LevelInfo
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})
	l := slog.New(h)
	slog.SetDefault(l)

	if pfxPath == "" {
		slog.Error("missing prefix path")
		os.Exit(1)
	}

	pfx, err := os.OpenRoot(pfxPath)
	if err != nil {
		slog.Error("failed to open prefix", "path", pfxPath, "reason", err)
		os.Exit(1)
	}

	var gc config.Global
	args := config.Args[config.NoHint]{
		Prefix: pfx,
		Global: nil,
		Hint:   nil,
	}
	err = config.Populate(&gc, args, l)
	if err != nil {
		slog.Error("failed to populate config", "reason", err)
		os.Exit(1)
	}

	var addDevs []miot.AddDeviceRequest
	if addDevices != "" {
		splitAddDevs := strings.Split(addDevices, ",")
		if len(splitAddDevs)%2 != 0 {
			slog.Error("invalid format for -a")
			os.Exit(1)
		}
		for i := range len(splitAddDevs) / 2 {
			addDevs = append(addDevs, miot.AddDeviceRequest{
				IPAddr: splitAddDevs[i],
				Token:  splitAddDevs[i+1],
			})
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	devArgs := miot.LoadArgs{
		Prefix:     pfx,
		Global:     &gc,
		Strict:     false,
		AddDevices: addDevs,
		Logger:     l,
	}
	devices, err := miot.LoadDevices(ctx, devArgs)
	if err != nil {
		slog.Error("failed to load devices", "reason", err)
		os.Exit(1)
	}

	rsv, _ := discovery.NewResolver()

	var wg sync.WaitGroup
	chDpMqtt := make(chan any)
	chMqttDp := make(chan any)
	ctxMq, cancelMq := context.WithCancel(context.Background())
	ctxDp, cancelDp := context.WithCancel(context.Background())

	broker, err := url.Parse(gc.MQTT.Endpoint)
	if err != nil {
		l.Error("parse url", "reason", err)
		os.Exit(1)
	}
	mqttArgs := ha.MQTTArgs{
		BrokerURL: *broker,
		Username:  gc.MQTT.Username,
		Password:  gc.MQTT.Password,
		Logger:    l.With("cmp", "mq"),
		FromDp:    chDpMqtt,
		ToDp:      chMqttDp,
		CancelDp:  cancelDp,
	}
	mq, err := ha.NewMQTT(ctx, mqttArgs)
	if err != nil {
		l.Error("init mqtt", "reason", err)
		os.Exit(1)
	}

	dpArgs := ha.DevicePoolArgs{
		FromMQTT: chMqttDp,
		ToMQTT:   chDpMqtt,
		Resolver: &rsv,
		Logger:   l.With("cmp", "pool"),
	}
	dp, err := ha.NewDevicePool(ctx, devices, dpArgs)
	if err != nil {
		l.Error("init dp", "reason", err)
		os.Exit(1)
	}

	// create detached contexts for services
	// since we need to do interdependent work on shutdown
	wg.Go(func() {
		mq.Subscribe(ctxMq)
	})

	wg.Go(func() {
		dp.Subscribe(ctxDp)
	})

	<-ctx.Done()
	cancelMq()
	wg.Wait()
	l.Info("terminated")
}
