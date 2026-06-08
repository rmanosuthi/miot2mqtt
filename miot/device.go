package miot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strings"
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

// A MapDevices is a map from [wire.DeviceID] to [Device]
// designed to be used by code that needs to:
//   - loop over all devices; or
//   - access a device's state through its ID
type MapDevices map[wire.DeviceID]Device

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

	// Map from [PropKey] to [SpecProp]. See [Device Properties].
	Props map[prop.PropKey]config.SpecProp

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

// ServiceName tries to find a Service associated with
// the URN with a Name of n.
// Meant to be used by HA.
func (dev *Device) ServiceName(n string) (Service, bool) {
	for _, svc := range dev.Services {
		if svc.Type.Name.Value() == n {
			return svc, true
		}
	}
	return Service{}, false
}

// FindPropKey tries to find a PropKey and a SpecProp
// given a service and the property's name.
// Service is used to only search for
// properties with a matching name belonging to
// the service.
func (dev *Device) FindPropKey(svc *Service, name string) (prop.Pair, bool) {
	for key, specProp := range dev.Props {
		if key.SIID == svc.IID && specProp.Urn.Name.Value() == name {
			return prop.Pair{Key: key, Spec: specProp}, true
		}
	}
	return prop.Pair{}, false
}

// AddDeviceRequest is a pair of unverified IP Address and Token strings,
// with its flag form being
//
//	IPAddr,Token
type AddDeviceRequest struct {
	IPAddr string
	Token  string
}

// AddDeviceRequests is a list of unverified IP Addresses and Token strings.
//
// It implements the [flag.Value] interface and each request
// is encoded as
//
//	IPAddr,Token
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

type ErrDeviceMerge struct {
	DeviceID string
	New      config.Device
	Existing config.Device
}

func (e ErrDeviceMerge) Error() string {
	return fmt.Sprintf("DeviceID %v already exists:\n%#v\nbut tried to add:\n%#v\n", e.DeviceID, e.Existing, e.New)
}

// miotDeviceArgs are arguments for [newDevice].
type miotDeviceArgs struct {
	DeviceID     wire.DeviceID
	Prefix       *os.Root
	Global       *config.Global
	Device       intermediateDevice
	GlobalLogger *slog.Logger
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
	props, err := prop.Parse(spec)
	if err != nil {
		return res, err
	}
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
		Props:      props,
		dialer:     dialer,
		timeStart:  timeStart,
		l:          l,
	}
	return res, err
}

// validateDeviceConfig checks that a [config.Device] is valid.
// It does not currently do anything.
func validateDeviceConfig(c *config.Device) error {
	return nil
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
