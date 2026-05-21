package miot

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strconv"
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
var ErrDeviceAdd = errors.New("failed to add device")
var ErrDeviceChanged = errors.New("device state changed, retry")

// A Device is a low-level representation of a device.
// All populated fields suffice to issue commands to one.
// The high-level equivalent is in [ha.Device].
//
// Initialization from outside this package can only be done through [LoadDevices]
// which operates on all devices defined in [config.Devices].
// See [newDevice] for internal initialization.
//
// Do not assume an initialized Device can always be communicated with.
// Certain devices restart themselves after some time if they cannot reach the cloud,
// and their timestamp epoch will need to be queried again.
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

	// Map from SIID to Service.
	Services Services

	// Map from URN to [ActionKey].
	ActionKeys ActionKeys
	// Map from [ActionKey] to [SpecAction].
	Actions Actions

	// Map from URN to [PropKey].
	PropKeys prop.PropKeys
	// Map from [PropKey] to [SpecProp]. See [Device Properties].
	Props prop.Props

	dialer net.Dialer
	// Devices have a second-precision timestamp with the epoch being
	// whenever they were turned on/last reset.
	//
	// timeStart is their epoch
	// from our perspective, used for generating proper timestamps.
	// The value is nil when a device couldn't be reached during
	// initialization.
	//
	// Methods that call the device must check for timeStart's nilness.
	timeStart *time.Time
	// Device-scope logger.
	l *slog.Logger
}

// PropName tries to find a [config.SpecProp] associated with
// the URN with a Name of n.
// Meant to be used by HA.
func (dev *Device) PropName(n string) (config.SpecProp, bool) {
	for urn, key := range dev.PropKeys {
		if urn.Name.Value() == n {
			res, ok := dev.Props[key]
			return res, ok
		}
	}
	return config.SpecProp{}, false
}

type LoadArgs struct {
	// Where the prefix given by -P is.
	Prefix *os.Root
	// Global config.
	Global *config.Global
	// When Strict, device initialization will fail if
	// it does not respond to a ping.
	// This effectively guarantees [Device.timestamp] will not be nil upon initialization,
	// but does not guarantee it will always be up-to-date.
	Strict bool
	// Devices to be added on [LoadDevices].
	// Successfully added devices will be committed to the config file.
	AddDevices []AddDeviceRequest
	// Logger to be used during load.
	Logger *slog.Logger
}

type miotDeviceArgs struct {
	DeviceID wire.DeviceID
	Prefix   *os.Root
	Global   *config.Global
	Device   intermediateDevice
	Logger   *slog.Logger
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
	l := args.Logger.With("did", args.DeviceID, "alias", dev.Alias, "addr", dev.IPAddr, "model", dev.Model)

	// is the token valid?
	token := wire.Token{}
	err := token.UnmarshalText([]byte(dev.Token))
	if err != nil {
		return res, fmt.Errorf("failed to parse token: %w", err)
	}

	services := parseServices(spec)
	l.Debug("parsed services")
	actionKeys, actions := parseActions(spec)
	l.Debug("parsed actions")
	propKeys, props := prop.Parse(spec)
	l.Debug("parsed props")

	// embed port
	addrPort := netip.AddrPortFrom(dev.IPAddr, wire.MiPort)

	var timeStart *time.Time
	// fetch timestamp
	var dialer net.Dialer
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pong, err := ping(pingCtx, &dialer, addrPort, args.Logger)
	if err != nil {
		l.Warn("device unreachable", "reason", err)
		err = errors.Join(ErrDevicePing, err)
	} else {
		l.Info("initialized device")
		timeStart = new(pong.Timestamp.EpochTime(time.Now()))
		l.Debug("when timestamp=0 time was", "time", timeStart)
	}

	res = Device{
		DeviceID:   args.DeviceID,
		Alias:      dev.Alias,
		Model:      dev.Model,
		Addr:       addrPort,
		Token:      token,
		Services:   services,
		ActionKeys: actionKeys,
		Actions:    actions,
		PropKeys:   propKeys,
		Props:      props,
		dialer:     dialer,
		timeStart:  timeStart,
		l:          l,
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

func populateSpec(
	ctx context.Context, dev parsedDevice,
	metaspecs []config.Metaspec, args LoadArgs,
) (config.Spec, error) {
	var spec config.Spec
	// find matching metaspec
	for _, metaspec := range metaspecs {
		if metaspec.Model == dev.Model && metaspec.Version == dev.Version {
			popArgs := config.Args[config.SpecHint]{
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
			err := config.Populate(&spec, popArgs, args.Logger)
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
//	 call [newDevice]
func LoadDevices(ctx context.Context, args LoadArgs) (MapDevices, error) {
	var cfgDevices config.Devices
	err := config.Populate(&cfgDevices, config.Args[config.NoHint]{
		Prefix: args.Prefix,
		Global: args.Global,
		Hint:   nil,
	}, args.Logger)
	if err != nil {
		return nil, err
	}

	// are there new devices to be added too?
	if len(args.AddDevices) > 0 {
		for _, dev := range args.AddDevices {
			dev, err := ResolveDevice(ctx, dev, args.Logger)
			if err != nil {
				return nil, err
			}

			// TODO don't default to Version 1
			// TODO don't return a "device already exists" error so late
			// workaround: backconvert did to string to
			// fit into cfgDevices
			didStr := strconv.Itoa(int(dev.DeviceID))
			if _, ok := cfgDevices[didStr]; ok {
				return nil, fmt.Errorf("%w: device already defined in config!", ErrDeviceAdd)
			}
			cfgDevices[didStr] = dev.Device
		}
		err := config.Flush(&cfgDevices, config.Args[config.NoHint]{
			Prefix: args.Prefix,
			Global: args.Global,
			Hint:   nil,
		}, args.Logger)
		if err != nil {
			return nil, ErrDeviceAdd
		}
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
		specArgs := config.Args[config.SpecHint]{
			Global: args.Global,
			Prefix: args.Prefix,
			Hint: &config.SpecHint{
				Model:    dev.Model,
				Version:  dev.Version,
				Download: nil,
			},
		}
		err := config.Load(&spec, specArgs, args.Logger)
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
		metaspecsArgs := config.Args[config.NoHint]{
			Prefix: args.Prefix,
			Global: args.Global,
			Hint:   nil,
		}
		err = config.Populate(&ms, metaspecsArgs, args.Logger)
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
		devArgs := miotDeviceArgs{
			DeviceID: did,
			Prefix:   args.Prefix,
			Global:   args.Global,
			Device:   dev,
			Logger:   args.Logger,
		}
		wg.Go(func() {
			dev, err := newDevice(initCtx, devArgs)
			devices <- deviceInit{
				Device: dev,
				Error:  err,
			}
		})
	}
	// devices is an unbuffered channel and
	// will block the above goroutine if we don't
	// drain it from below
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

// UpdateTimestamp calibrates the device's epoch to curr.
func (dev *Device) UpdateTimestamp(curr wire.Timestamp) error {
	// FIXME This isn't needed in practice? Investigate further.
	// Device seems to be ok with our timestamp even after it has
	// reset itself about every hour, but restarting the program
	// would get a new, more recent epoch.
	//
	// TODO capture pcap around when the hour mark passes.
	epoch := curr.EpochTime(time.Now())
	dev.timeStart = &epoch
	dev.l.Debug("updated timestamp", "epoch", epoch)
	return ErrDeviceChanged
}
