package miot

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrDeviceInit = errors.New("failed to initialize device handle")
var ErrDeviceDial = errors.New("failed to dial device")
var ErrDeviceSend = errors.New("failed to send to device")
var ErrDeviceRecv = errors.New("failed to receive from device")
var ErrDevicePing = errors.New("failed to ping device")
var ErrDeviceUninit = errors.New("device not initialized")

// A Device is a low-level representation of a device.
// All populated fields suffice to issue commands to one.
//
// The high-level equivalent is in [ha.Device].
//
// Initialization from outside this package can only be done through [LoadDevices]
// which operates on all devices defined in [config.Devices].
//
// See [newMiotDevice] for internal initialization.
type Device struct {
	// Device identifier.
	DeviceID wire.DeviceID
	// User-friendly name, see [config.Device].
	Alias string
	// Model name, usually formatted "a.b.c".
	Model string
	// Address and port to communicate with device.
	Addr netip.AddrPort
	// Encryption and decryption object.
	Token wire.Token
	// Spec resolved through [Metaspec].
	Spec    config.Spec
	Actions map[config.URN]ActionKey
	// Properties. See [Device Properties].
	Props map[config.URN]prop.PropKey

	dialer net.Dialer
	// Devices have a second-precision timestamp with the epoch being
	// whenever they were turned on/last reset.
	//
	// timeStart is their epoch
	// from our perspective, used for generating proper timestamps.
	// The value is nil when a device couldn't be reached during
	// initialization.
	timeStart *time.Time
}

type LoadArgs struct {
	Prefix    *os.Root
	Global    *config.Global
	Strict    bool
	AddDevice string
}

type miotDeviceArgs struct {
	DeviceID wire.DeviceID
	Prefix   *os.Root
	Global   *config.Global
	Device   intermediateDevice
}

// newDevice initializes a MiotDevice.
// This function will return both a MiotDevice and ErrDevicePing if the device couldn't be reached.
func newDevice(ctx context.Context, args miotDeviceArgs) (Device, error) {
	var res Device

	// technically we don't even need metaspec if spec file
	// already exists and cfgDevice.Version is defined,
	// but keep it simple instead of special casing it
	dev := &args.Device
	spec := &dev.Spec
	l := slog.Default().With("did", args.DeviceID, "alias", dev.Alias, "addr", dev.IPAddr, "model", dev.Model)

	// is the token valid?
	token := wire.Token{}
	err := token.UnmarshalText([]byte(dev.Token))
	if err != nil {
		return res, fmt.Errorf("failed to parse token: %w", err)
	}

	actions := parseActions(spec)
	l.Debug("parsed actions")
	props := prop.ParseFrom(spec)
	l.Debug("parsed props")

	// embed port
	addrPort := netip.AddrPortFrom(dev.IPAddr, wire.MiPort)

	var timeStart *time.Time
	// fetch timestamp
	var dialer net.Dialer
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pong, err := ping(pingCtx, &dialer, addrPort)
	if err != nil {
		l.Warn("device unreachable", "reason", err)
		err = errors.Join(ErrDevicePing, err)
	} else {
		l.Info("initialized device")
		timeStart = new(pong.Timestamp.EpochTime(time.Now()))
		l.Debug("when timestamp=0 time was", "time", timeStart)
	}

	res = Device{
		DeviceID:  args.DeviceID,
		Alias:     dev.Alias,
		Model:     dev.Model,
		Addr:      addrPort,
		Token:     token,
		Spec:      *spec,
		Actions:   actions,
		Props:     props,
		dialer:    dialer,
		timeStart: timeStart,
	}
	return res, err
}

// A MapDevices is a map from [wire.DeviceID] to [Device]
// designed to be used by code that needs to:
//   - loop over all devices; or
//   - access a device's state through its ID
type MapDevices map[wire.DeviceID]Device

// TODO
func validateDeviceConfig(c *config.Device) error {
	return nil
}

type parsedDevice struct {
	config.Device
}

type parsedDevices map[wire.DeviceID]parsedDevice

type intermediateDevice struct {
	config.Device
	config.Spec
}

type intermediateDevices map[wire.DeviceID]intermediateDevice

func populateSpec(ctx context.Context, dev parsedDevice, metaspecs []config.Metaspec, args LoadArgs) (config.Spec, error) {
	var spec config.Spec
	// find matching metaspec
	for _, metaspec := range metaspecs {
		if metaspec.Model == dev.Model && metaspec.Version == dev.Version {
			args := config.Args[config.SpecHint]{
				Prefix: args.Prefix,
				Global: args.Global,
				Hint: &config.SpecHint{
					Model:   metaspec.Model,
					Version: metaspec.Version,
					Download: &config.SpecDownload{
						URN:     metaspec.SpecURN,
						Context: ctx,
					},
				},
			}
			err := config.Populate(&spec, args)
			return spec, err
		}
	}
	return spec, ErrNoMetaspec
}

func parseDevices(devs config.Devices) (parsedDevices, error) {
	res := make(parsedDevices)
	for didStr, cfgDevice := range devs {
		if !cfgDevice.Enabled {
			slog.Debug("found disabled device", "did", didStr)
			continue
		}
		var didRaw uint32
		n, err := fmt.Sscanf(didStr, "%d", &didRaw)
		if n != 1 {
			return nil, fmt.Errorf("%w: failed to read did", ErrDeviceInit)
		}
		if err != nil {
			return nil, err
		}
		did := wire.DeviceID(didRaw)

		// validate
		err = validateDeviceConfig(&cfgDevice)
		if err != nil {
			return nil, errors.Join(ErrDeviceInit, err)
		}

		res[did] = parsedDevice{cfgDevice}
	}
	return res, nil
}

type resolveDeviceResult struct {
	config.Device
	DeviceID wire.DeviceID
}

func resolveDevice(ctx context.Context, args LoadArgs, dev string) (resolveDeviceResult, error) {
	var res resolveDeviceResult
	// expect form "ip,tokenhex"
	segs := strings.Split(args.AddDevice, ",")
	if len(segs) != 2 {
		return res, fmt.Errorf("%w: wrong segment len %v", ErrDeviceInit, len(segs))
	}

	addr, err := netip.ParseAddr(segs[0])
	if err != nil {
		return res, errors.Join(ErrDeviceInit, err)
	}

	var tokenBytes [16]byte
	tokenLen, err := hex.Decode(tokenBytes[:], []byte(segs[1]))
	if err != nil {
		return res, errors.Join(ErrDeviceInit, err)
	}
	if tokenLen != wire.TokenLen {
		return res, fmt.Errorf("%w: wrong token len %v", ErrDeviceInit, err)
	}
	token, err := wire.NewToken(tokenBytes)
	if err != nil {
		return res, errors.Join(ErrDeviceInit, err)
	}

	devInfo, err := ResolveFromIpToken(ctx, addr, token)
	if err != nil {
		return res, errors.Join(ErrDeviceInit, err)
	}

	res.IPAddr = addr
	res.Model = devInfo.Model
	res.Token = segs[1]
	res.DeviceID = devInfo.DeviceID
	res.Version = 1
	res.Enabled = true
	return res, nil
}

// LoadDevices loads devices' states from disk.
// Metaspecs may be loaded for devices without a spec file.
// ctx is only used to cancel initialization and is not stored.
//
// The lifecycle of a [Device] is as follows:
//
//	[config.Devices] loaded
//	for each [config.Device]:
//	 - validate
//	 - load spec
//	 - if found, return as [intermediateDevice]
//	 - else, defer loading
//	if deferred devices present, load metaspec
//	for each deferred device:
//	 - populate spec from metaspec
//	 - return as [intermediateDevice]
//	parallel for each [intermediateDevice]:
//	 call [newMiotDevice]
func LoadDevices(ctx context.Context, args LoadArgs) (MapDevices, error) {
	var cfgDevices config.Devices
	err := config.Populate(&cfgDevices, config.Args[config.NoHint]{
		Prefix: args.Prefix,
		Global: args.Global,
		Hint:   nil,
	})
	if err != nil {
		return nil, err
	}

	// are there new devices to be added too?
	if args.AddDevice != "" {
		dev, err := resolveDevice(ctx, args, args.AddDevice)
		if err != nil {
			return nil, err
		}

		// TODO don't default to Version 1
		// TODO don't return a "device already exists" error so late
		// workaround: backconvert did to string to
		// fit into cfgDevices
		didStr := strconv.Itoa(int(dev.DeviceID))
		if _, ok := cfgDevices[didStr]; ok {
			return nil, fmt.Errorf("device already defined in config!")
		}
		cfgDevices[didStr] = dev.Device
		config.Flush(&cfgDevices, config.Args[config.NoHint]{
			Prefix: args.Prefix,
			Global: args.Global,
			Hint:   nil,
		})
	}
	slog.Debug("devices pass 1: populate", "found", len(cfgDevices))

	devs, err := parseDevices(cfgDevices)
	if err != nil {
		return nil, err
	}
	slog.Debug("devices pass 2: parse", "found", len(devs))

	deferredDevices := make(parsedDevices)
	deviceModels := make(intermediateDevices)

	// load devices with specs already present
	for did, dev := range devs {
		var spec config.Spec
		args := config.Args[config.SpecHint]{
			Global: args.Global,
			Prefix: args.Prefix,
			Hint: &config.SpecHint{
				Model:    dev.Model,
				Version:  dev.Version,
				Download: nil,
			},
		}
		err := config.Load(&spec, args)
		if err != nil && errors.Is(err, fs.ErrNotExist) {
			slog.Warn("device has no spec, will populate from metaspec (slow)", "did", did)
			deferredDevices[did] = dev
		} else if err != nil {
			return nil, err
		} else {
			deviceModels[did] = intermediateDevice{
				Device: dev.Device,
				Spec:   spec,
			}
		}
	}
	slog.Debug("devices pass 3: load those with specs",
		"with", len(deviceModels),
		"without", len(deferredDevices),
	)

	if len(deferredDevices) > 0 {
		// populate metaspecs
		var ms config.Metaspecs
		msargs := config.Args[config.NoHint]{
			Prefix: args.Prefix,
			Global: args.Global,
			Hint:   nil,
		}
		err = config.Populate(&ms, msargs)
		if err != nil {
			return nil, fmt.Errorf("failed to populate metaspecs: %w", err)
		}

		for did, dev := range deferredDevices {
			spec, err := populateSpec(ctx, dev, ms.Instances, args)
			if err != nil {
				return nil, err
			}
			deviceModels[did] = intermediateDevice{
				Device: dev.Device,
				Spec:   spec,
			}
		}
		slog.Debug("devices pass 3a: metaspecs")
	}

	// devices can take a while to respond (or not)
	// init devices in parallel
	var wg sync.WaitGroup
	type deviceInit struct {
		Device Device
		Error  error
	}
	devices := make(chan deviceInit)
	initCtx, cancelInit := context.WithCancel(ctx)
	defer cancelInit()

	for did, dev := range deviceModels {
		args := miotDeviceArgs{
			DeviceID: did,
			Prefix:   args.Prefix,
			Global:   args.Global,
			Device:   dev,
		}
		wg.Go(func() {
			dev, err := newDevice(initCtx, args)
			devices <- deviceInit{
				Device: dev,
				Error:  err,
			}
		})
	}
	go func() {
		wg.Wait()
		close(devices)
	}()

	res := make(MapDevices)
	for devInit := range devices {
		slog.Debug("initDevice")
		err := devInit.Error
		if !args.Strict {
			if err != nil && !errors.Is(err, ErrDevicePing) {
				// error is too severe, don't register device
				slog.Warn("not registering device", "reason", err)
			} else {
				// register device even though it could be offline
				res[devInit.Device.DeviceID] = devInit.Device
			}
		} else {
			if err != nil {
				// give up
				return nil, errors.Join(ErrDeviceInit, err)
			}
			res[devInit.Device.DeviceID] = devInit.Device
		}
	}

	slog.Debug("loaded devices", "count", len(res))
	if len(res) == 0 {
		slog.Warn("no devices")
	}
	return res, nil
}
