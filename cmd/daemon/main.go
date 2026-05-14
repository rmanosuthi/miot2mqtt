package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/device"
	"github.com/rmanosuthi/miot2mqtt/ha"
)

func main() {
	ctx := context.Background()
	var pfxPath, mode, inputFile, msg string
	var verbose, save bool
	flag.StringVar(&pfxPath, "P", "", "path to prefix")
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.StringVar(&mode, "m", "", "operation mode")
	flag.StringVar(&inputFile, "f", "", "input file")
	flag.BoolVar(&save, "s", false, "save state")
	flag.StringVar(&msg, "i", "", "arbitrary message")
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
	err = config.Populate(&gc, args)
	if err != nil {
		slog.Error("failed to populate config", "reason", err)
		os.Exit(1)
	}

	mqttDebugHandler := slog.NewTextHandler(os.Stderr, nil)
	mqttErrorHandler := slog.NewTextHandler(os.Stderr, nil)
	mqttDebug := slog.NewLogLogger(mqttDebugHandler, logLevel)
	mqttError := slog.NewLogLogger(mqttErrorHandler, logLevel)

	/* register sigint */
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	devArgs := device.LoadArgs{
		Prefix: pfx,
		Global: &gc,
		Strict: false,
	}
	devices, err := device.LoadDevices(ctx, devArgs)
	if err != nil {
		slog.Error("failed to load devices", "reason", err)
		os.Exit(1)
	}

	ctxHa, cancelHa := context.WithCancel(ctx)
	defer cancelHa()
	conn, err := ha.NewConnection(ctxHa, l, &gc, mqttDebug, mqttError)
	if err != nil {
		slog.Error("failed to initialize HA", "reason", err)
		os.Exit(1)
	}
	haRes := make(chan error)
	go func() {
		err := conn.Consume(ctxHa, ha.HaConsume{
			DeviceMap: devices,
		})
		haRes <- err
	}()

	select {
	case <-c:
		slog.Info("stopping HA")
		cancelHa()
		<-haRes
		return
	case err := <-haRes:
		if err != nil {
			slog.Error("HA exited with error", "reason", err)
			os.Exit(1)
		}
		return
	}
}
