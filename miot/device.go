package miot

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"net"
	"net/netip"
	"os"
	"slices"
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
	// Device class. Extracted from spec's toplevel URN.
	Class string
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

// AddDeviceRequest is a pair of unverified IP Address and Token strings.
type AddDeviceRequest struct {
	IPAddr string
	Token  string
}

// AddDeviceRequests is a list of unverified IP Addresses and Token strings.
//
// It implements the [flag.Value] interface.
type AddDeviceRequests []AddDeviceRequest

func (adr *AddDeviceRequests) String() string {
	return ""
}

func (adr *AddDeviceRequests) Set(value string) error {
	v := strings.TrimSpace(value)
	subs := strings.Split(v, ",")
	if subs[0] == "" {
		return fmt.Errorf("no address")
	}
	if subs[1] == "" {
		return fmt.Errorf("no token")
	}

	*adr = append(*adr, AddDeviceRequest{
		IPAddr: subs[0],
		Token:  subs[1],
	})
	return nil
}

// AddDevicesArgs are arguments for [DevicesToAdd].
type AddDeviceArgs struct {
	Prefix       *os.Root
	Global       *config.Global
	GlobalLogger *slog.Logger
	Reqs         AddDeviceRequests
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
	// In-memory config devices may be given to
	// merge with the existing config.
	// Designed to be used with [DevicesToAdd].
	MergeWith config.Devices
	// GlobalLogger to be used during load.
	GlobalLogger *slog.Logger
}

type miotDeviceArgs struct {
	DeviceID     wire.DeviceID
	Prefix       *os.Root
	Global       *config.Global
	Device       intermediateDevice
	GlobalLogger *slog.Logger
}

type ErrDeviceMerge struct {
	DeviceID string
	New      config.Device
	Existing config.Device
}

func (e ErrDeviceMerge) Error() string {
	return fmt.Sprintf("DeviceID %v already exists:\n%#v\nbut tried to add:\n%#v\n", e.DeviceID, e.Existing, e.New)
}

// newDevice initializes a MiotDevice.
// This function will return both a MiotDevice and ErrDevicePing if the device couldn't be reached.
func newDevice(ctx context.Context, args miotDeviceArgs) (Device, error) {
	var res Device

	dev := &args.Device
	spec := &dev.Spec
	l := args.GlobalLogger.WithGroup("miot").With("did", args.DeviceID, "addr", dev.IPAddr)

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
	pong, err := ping(pingCtx, &dialer, addrPort, args.GlobalLogger)
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
		Class:      spec.Type.Name.Value(),
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
				Perm: 0o644,
			}
			err := config.Populate(&spec, popArgs, args.GlobalLogger)
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

// DevicesToAdd converts each unparsed IP-Token entry into
// [config.Device] and [config.Metaspec].
//
// The device config is not changed nor are [config.Spec] downloaded,
// but [config.Metaspecs] may be downloaded.
func DevicesToAdd(ctx context.Context, args AddDeviceArgs) (config.DevicesMeta, error) {
	l := args.GlobalLogger
	res := make(config.DevicesMeta)
	// load metaspecs, will need them to determine version
	var ms config.Metaspecs
	metaspecsArgs := config.Args[config.NoHint]{
		Prefix: args.Prefix,
		Global: args.Global,
		Hint:   nil,
		Perm:   0o644,
	}
	err := config.Populate(&ms, metaspecsArgs, args.GlobalLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to populate metaspecs: %w", err)
	}

	for _, addDevReq := range args.Reqs {
		resolvedDev, err := ResolveDevice(ctx, addDevReq, args.GlobalLogger)
		if err != nil {
			return nil, err
		}

		// workaround: backconvert did to string
		// for correct key type
		didStr := strconv.Itoa(int(resolvedDev.Info.DeviceID))

		// Match whether device has already been defined by its DeviceID
		// rather than IP or token.
		// Unfortunately this can't be done earlier and we
		// must have already contacted the device to do it.
		metaspecs := slices.Values(ms.Instances)
		meta, err := ResolveDefaultMetaspec(resolvedDev.Info.Model, metaspecs,
			// NOTE potential optimization:
			// just reuse another device with the same model's Version
			func(a config.Metaspec, b config.Metaspec) int {
				return cmp.Compare(a.Version, b.Version)
			},
		)
		if err != nil {
			return nil, err
		}

		cfgDev := resolvedDev.WithVersion(&meta)
		l.Debug("device", "cfg", cfgDev)
		res[didStr] = config.DeviceMeta{
			Device: cfgDev,
			Meta:   meta,
		}
	}

	l.Debug("devices to be added", "count", len(res))
	return res, nil
}

// LoadDevices loads devices' states from disk.
// Metaspecs may be loaded for devices without a spec file.
// ctx is only used to cancel initialization and is not stored.
//
// The following steps outline the initialization process:
//
//	load [config.Devices]
//	if args.MergeWith is present:
//	 - check for DID collisions
//	 - merge into [config.Devices]
//	 - save config to disk
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
	l := args.GlobalLogger
	var cfgDevices config.Devices
	err := config.Populate(&cfgDevices, config.Args[config.NoHint]{
		Prefix: args.Prefix,
		Global: args.Global,
		Hint:   nil,
		// this file may contain tokens
		Perm: 0o600,
	}, args.GlobalLogger)
	if err != nil {
		return nil, err
	}

	if len(cfgDevices) == 0 && len(args.MergeWith) == 0 {
		return nil, fmt.Errorf("no devices")
	}

	// are there new devices to be added too?
	if len(args.MergeWith) > 0 {
		// check if a device with the same DeviceID already exists
		for did, mergeDev := range args.MergeWith {
			if existingDev, ok := cfgDevices[did]; ok {
				return nil, errors.Join(ErrDeviceAdd, ErrDeviceMerge{
					DeviceID: did,
					New:      mergeDev,
					Existing: existingDev,
				})
			}
		}

		// merge the two
		l.Debug("load devices: merging", "count", len(args.MergeWith))
		maps.Copy(cfgDevices, args.MergeWith)

		err := config.Flush(&cfgDevices, config.Args[config.NoHint]{
			Prefix: args.Prefix,
			Global: args.Global,
			Hint:   nil,
		}, args.GlobalLogger)
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
		err := config.Load(&spec, specArgs, args.GlobalLogger)
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
			Perm:   0o644,
		}
		err = config.Populate(&ms, metaspecsArgs, args.GlobalLogger)
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
			DeviceID:     did,
			Prefix:       args.Prefix,
			Global:       args.Global,
			Device:       dev,
			GlobalLogger: args.GlobalLogger,
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

	if args.Strict {
		l.Debug("strict device loading")
	} else {
		l.Debug("relaxed device loading")
	}

	res := make(MapDevices)
	for devInit := range devices {
		slog.Debug("initDevice")
		err := devInit.Error
		if err != nil {
			if args.Strict {
				return res, err
			} else {
				if errors.Is(err, ErrDevicePing) {
					l.Warn("device offline", "reason", err)
					// register device even though it could be offline
					res[devInit.Device.DeviceID] = devInit.Device
				} else {
					// error is too severe, don't register device
					l.Warn("skipping device", "reason", err)
				}
			}
		} else {
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
