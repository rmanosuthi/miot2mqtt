/*
Utils is a collection of utilities for miot2mqtt.

Usage:

	utils [mode] -P prefix [-v] [opts]

The modes are:

	dev
		Device related functions.

	pcap
		Packet capture related functions.

See [device] and [pcap] for mode specific usages.

opts depends on the chosen mode.
Two flags are always recognized.
However, they must always come after mode and
not before.

	-P prefix
		`prefix` specifies the path to the instance's prefix.
		It must always be given.

	-v
		Enable verbose logging.
*/
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rmanosuthi/miot2mqtt/cmd/utils/common"
	"github.com/rmanosuthi/miot2mqtt/cmd/utils/device"
	"github.com/rmanosuthi/miot2mqtt/cmd/utils/pcap"
)

func main() {
	logHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: common.LogLevel,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		flag.Usage()
		return
	}
	mode := os.Args[1]
	subargs := os.Args[2:]

	if mode == "" {
		logger.Error("no mode")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	switch mode {
	case "dev":
		l := logger.WithGroup("dev")
		err := device.Entrypoint(ctx, l, subargs)
		if err != nil {
			l.Error("exiting with error", "reason", err)
			os.Exit(1)
		}
	case "pcap":
		l := logger.WithGroup("pcap")
		err := pcap.Entrypoint(ctx, l, subargs)
		if err != nil {
			l.Error("exiting with error", "reason", err)
			os.Exit(1)
		}
	default:
		logger.Error("unrecognized mode", "mode", mode)
		os.Exit(1)
	}
}
