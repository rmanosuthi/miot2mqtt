package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha"
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

	mqttDebugHandler := slog.NewTextHandler(os.Stderr, nil)
	mqttErrorHandler := slog.NewTextHandler(os.Stderr, nil)
	mqttDebug := slog.NewLogLogger(mqttDebugHandler, logLevel)
	mqttError := slog.NewLogLogger(mqttErrorHandler, logLevel)

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

	conn, err := ha.NewConnection(ctx, l, &gc, mqttDebug, mqttError)
	if err != nil {
		slog.Error("failed to initialize HA", "reason", err)
		os.Exit(1)
	}
	err = conn.Consume(ctx, ha.HaConsume{
		DeviceMap: devices,
	})
	if err != nil {
		slog.Error("HA exited with error", "reason", err)
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
