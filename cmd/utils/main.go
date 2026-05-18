package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/ha"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

func main() {
	var pfxPath, mode, inputFile, msg string
	var verbose, save bool
	flag.StringVar(&pfxPath, "P", "", "path to prefix")
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.StringVar(&mode, "m", "", "operation mode")
	flag.StringVar(&inputFile, "f", "", "input file")
	flag.BoolVar(&save, "s", false, "save state")
	flag.StringVar(&msg, "i", "", "arbitrary message")
	flag.Parse()

	ctx := context.Background()
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
	if mode == "" {
		slog.Error("missing mode")
		os.Exit(1)
	}

	pfx, err := os.OpenRoot(pfxPath)
	if err != nil {
		slog.Error("failed to open prefix", "path", pfxPath, "reason", err)
		os.Exit(1)
	}
	gc := new(config.Global)
	globalArgs := config.Args[config.NoHint]{
		Prefix: pfx,
		Global: nil,
		Hint:   nil,
	}
	err = config.Populate(gc, globalArgs, l)
	if err != nil {
		slog.Error("failed to populate config", "reason", err)
		os.Exit(1)
	}

	var ms config.Metaspecs
	err = config.Populate(&ms, globalArgs, l)
	if err != nil {
		slog.Error("failed to populate metaspecs", "reason", err)
		os.Exit(1)
	}

	devArgs := miot.LoadArgs{
		Prefix: pfx,
		Global: gc,
		Strict: false,
		Logger: l,
	}
	devices, err := miot.LoadDevices(ctx, devArgs)
	if err != nil {
		slog.Error("failed to load devices", "reason", err)
		os.Exit(1)
	}

	switch mode {
	case "add":
		args := strings.Split(msg, ":")
		addr, _ := netip.ParseAddr(args[0])
		var tokenBytes [16]byte
		hex.Decode(tokenBytes[:], []byte(args[1]))
		token, _ := wire.NewToken(tokenBytes)

		info, err := miot.ResolveFromIPToken(context.TODO(), addr, token, l)
		if err != nil {
			slog.Error("failed to add device", "reason", err)
			os.Exit(1)
		}
		fmt.Printf("%v\n", info)

		metaspecs := slices.Values(ms.Instances)
		meta, err := miot.ResolveDefaultMetaspec(info.Model, metaspecs, func(a config.Metaspec, b config.Metaspec) int {
			if a.Version < b.Version {
				return -1
			} else if a.Version > b.Version {
				return 1
			} else {
				return 0
			}
		})
		if err != nil {
			slog.Error("meta", "reason", err)
			os.Exit(1)
		}

		var spec config.Spec
		a := config.Args[config.SpecHint]{
			Prefix: pfx,
			Global: gc,
			Hint: &config.SpecHint{
				Model:   meta.Model,
				Version: meta.Version,
				Download: &config.SpecDownload{
					URN:     meta.SpecURN,
					Context: ctx,
				},
			},
		}
		err = config.Populate(&spec, a, l)
		if err != nil {
			slog.Error("populate", "reason", err)
			os.Exit(1)
		}
	case "devices":
		for did, device := range devices {
			fmt.Printf("did %#x:\n", did)
			fmt.Printf("%#v\n", device)
			for actionName, action := range device.Actions {
				fmt.Printf("did %#x action %v:\n", did, actionName)
				fmt.Printf("%#v\n", action)
			}
			for propName, action := range device.Props {
				fmt.Printf("did %#x prop %v:\n", did, propName)
				fmt.Printf("%#v\n", action)
			}
		}
	case "pcap":
		replayPcap(devices, inputFile, verbose, true)
	case "props":
		for did, dev := range devices {
			propCtx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			props, err := dev.GetProperties(propCtx, func(urn config.URN, key prop.PropKey) bool {
				return key.SIID == 2
			})
			if err != nil {
				slog.Warn("failed to hello device", "reason", err)
				continue
			}
			for urn, prop := range props {
				if prop.Error == nil {
					fmt.Printf("[%v] %v: %v\n", did, urn, prop.Response.Value)
				}
			}
		}
	case "set":
		/*did := wire.DeviceID(0) // TODO real value
		dev := devices[did]
		propCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		key, err := prop.NewSetProp(dev.Props["on"], true)
		if err != nil {
			slog.Error("no key")
			os.Exit(1)
		}
		props, err := dev.SetProperties(propCtx, map[string]*device.SetPropKey{
			"test": key,
		})
		if err != nil {
			slog.Error("set fail", "reason", err)
			os.Exit(1)
		}
		for propName, prop := range props {
			if prop != nil {
				fmt.Printf("[%v] %v: %v\n", did, propName, prop.Value)
			}
		}*/
	case "hareg":
		for did, dev := range devices {
			hdev, err := ha.InitDevice(dev)
			if err != nil {
				slog.Error("ha init fail", "reason", err)
				continue
			}
			jb, err := hdev.Discovery()
			if err != nil {
				slog.Error("ha reg fail", "reason", err)
				continue
			}
			fmt.Printf("%v: %v\n", did, string(jb))
		}
	default:
		slog.Error("unrecognized mode", "mode", mode)
		os.Exit(1)
	}
}
