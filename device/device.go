package device

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/device/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrDeviceInit = errors.New("failed to initialize device handle")
var ErrDeviceDial = errors.New("failed to dial device")
var ErrDeviceSend = errors.New("failed to send to device")
var ErrDeviceRecv = errors.New("failed to receive from device")
var ErrDevicePing = errors.New("failed to ping device")
var ErrDeviceUninit = errors.New("device not initialized")

// A MiotDevice is a low-level representation of a device.
// All populated fields suffice to issue commands to one.
//
// The high-level equivalent is in [ha.Device].
//
// Initialization from outside this package can only be done through [LoadDevices]
// which operates on all devices defined in [config.Devices].
//
// See [newMiotDevice] for internal initialization.
type MiotDevice struct {
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
	Actions map[config.Urn]ActionKey
	// Properties. See [Device Properties].
	Props map[config.Urn]prop.PropKey

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

type miotDeviceArgs struct {
	did    wire.DeviceID
	pfx    *os.Root
	gc     *config.Global
	device intermediateDevice
}

// newMiotDevice initializes a MiotDevice.
// This function will return both a MiotDevice and ErrDevicePing if the device couldn't be reached.
func newMiotDevice(ctx context.Context, args miotDeviceArgs) (MiotDevice, error) {
	var res MiotDevice

	// technically we don't even need metaspec if spec file
	// already exists and cfgDevice.Version is defined,
	// but keep it simple instead of special casing it
	dev := &args.device
	spec := &dev.Spec
	l := slog.Default().With("did", args.did, "alias", dev.Alias, "addr", dev.IPAddr)

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
		timeStart = new(pong.Timestamp.EpochTime(time.Now()))
		l.Debug("when timestamp=0 time was", "time", timeStart)
	}

	// TODO better verification
	res = MiotDevice{
		DeviceID:  args.did,
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

// A MapDevices is a map from [wire.DeviceID] to [MiotDevice]
// designed to be used by code that needs to:
//   - loop over all devices; or
//   - access a device's state through its ID
type MapDevices map[wire.DeviceID]MiotDevice

func validateDeviceConfig(c *config.Device) error {
	if c.Model == "" {
		slog.Warn("device model undefined, contacting it")
		// TODO
	}
	// don't deal with Version yet, need metaspecs
	return nil
}

type intermediateDevice struct {
	config.Device
	config.Spec
}

type intermediateDevices map[wire.DeviceID]intermediateDevice

// LoadDevices loads devices' states from disk.
// It must be given metaspecs to resolve.
// ctx is only used to cancel initialization and is not stored.
//
// The lifecycle of a [MiotDevice] is as follows:
//
//	[config.Devices] loaded
//	for each [config.Device]:
//	 - validate
//	 - find matching [Metaspec]
//	 - [Populate] the [Spec]
//	 - return as [intermediateDevice]
//	parallel for each [intermediateDevice]:
//	 call [newMiotDevice]
func LoadDevices(ctx context.Context, pfx *os.Root, gc *config.Global,
	metaspecs []config.Metaspec, strict bool) (MapDevices, error) {
	var cfgDevices config.Devices
	err := config.Populate(&cfgDevices, config.Args[config.NoHint]{
		Prefix: pfx,
		Global: gc,
		Hint:   nil,
	})
	if err != nil {
		return nil, err
	}
	slog.Debug("devices pass 1: populate", "found", len(cfgDevices))

	// build lookup table first
	deviceModels := make(intermediateDevices)

	// config validation pass
	for didStr, cfgDevice := range cfgDevices {
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

		var spec config.Spec
		// find matching metaspec
		for _, metaspec := range metaspecs {
			if metaspec.Model == cfgDevice.Model && metaspec.Version == cfgDevice.Version {
				args := config.Args[config.Metaspec]{
					Prefix: pfx,
					Global: gc,
					Hint:   &metaspec,
				}
				err := config.Populate(&spec, args)
				if err != nil {
					return nil, err
				}
			}
		}
		deviceModels[did] = intermediateDevice{
			Device: cfgDevice,
			Spec:   spec,
		}
	}
	slog.Debug("devices pass 2: config, specs", "found", len(deviceModels))

	// devices can take a while to respond (or not)
	// init devices in parallel
	var wg sync.WaitGroup
	type deviceInit struct {
		Device MiotDevice
		Error  error
	}
	devices := make(chan deviceInit)
	initCtx, cancelInit := context.WithCancel(ctx)
	defer cancelInit()

	for did, dev := range deviceModels {
		args := miotDeviceArgs{
			did: did,
			pfx: pfx, gc: gc,
			device: dev,
		}
		wg.Go(func() {
			dev, err := newMiotDevice(initCtx, args)
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
		if !strict {
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
