package common

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot"
)

var LogLevel = new(slog.LevelVar)

// MinInstance is the minimal state all important functionalities of
// miot2mqtt need.
type MinInstance struct {
	PrefixRoot *os.Root
	Global     config.Global
	ModeLogger *slog.Logger
}

// FullInstance is a superset of MinInstance.
type FullInstance struct {
	MinInstance
	Metaspecs config.Metaspecs
	Devices   miot.MapDevices
}

type GlobalFlags struct {
	Verbose bool
	Prefix  string
}

func Flags(dst *GlobalFlags, fs *flag.FlagSet) {
	fs.BoolVar(&dst.Verbose, "v", false, "Log verbosity")
	fs.StringVar(&dst.Prefix, "P", "", "Prefix path")
}

func MinInit(ctx context.Context, logger *slog.Logger, gf *GlobalFlags) (MinInstance, error) {
	if gf.Verbose {
		LogLevel.Set(slog.LevelDebug)
	}

	pfx, err := os.OpenRoot(gf.Prefix)
	if err != nil {
		return MinInstance{}, fmt.Errorf("open prefix: %w", err)
	}

	var global config.Global
	globalArgs := config.Args[config.NoHint]{
		Prefix: pfx,
		Global: nil,
		Hint:   nil,
	}
	err = config.Populate(&global, globalArgs, logger)
	if err != nil {
		return MinInstance{}, fmt.Errorf("populate config: %w", err)
	}

	return MinInstance{
		PrefixRoot: pfx,
		Global:     global,
		ModeLogger: logger,
	}, nil
}

func FullInit(ctx context.Context, mini MinInstance) (FullInstance, error) {
	var metaspecs config.Metaspecs
	globalArgs := config.Args[config.NoHint]{
		Prefix: mini.PrefixRoot,
		Global: nil,
		Hint:   nil,
	}
	err := config.Populate(&metaspecs, globalArgs, mini.ModeLogger)
	if err != nil {
		return FullInstance{}, fmt.Errorf("populate metaspecs: %w", err)
	}

	devArgs := miot.LoadArgs{
		Prefix:       mini.PrefixRoot,
		Global:       &mini.Global,
		Strict:       false,
		GlobalLogger: mini.ModeLogger,
	}
	devices, err := miot.LoadDevices(ctx, devArgs)
	if err != nil {
		return FullInstance{}, fmt.Errorf("load devices: %w", err)
	}

	return FullInstance{
		MinInstance: mini,
		Metaspecs:   metaspecs,
		Devices:     devices,
	}, nil
}
