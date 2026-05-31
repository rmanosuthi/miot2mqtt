package miot

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

const (
	StatusReleased = "released"
	StatusDebug    = "debug"
)

var ErrDeviceResolve = errors.New("resolve device")
var ErrDeviceDig = errors.New("failed to get device info")
var ErrNoMetaspec = errors.New("model has no metaspec")

// ResolvedDevice contains almost all information
// necessary to operate on a device except for the spec's version.
type ResolvedDevice struct {
	Info Info
	// Do not access this field directly.
	// Call [ResolvedDevice.WithVersion] to produce a valid config.
	cfgDev config.Device
}

func (rd *ResolvedDevice) WithVersion(meta *config.Metaspec) config.Device {
	newDev := rd.cfgDev

	newDev.Version = meta.Version
	return newDev
}

type miQueryInfo struct {
	ID     uint32   `json:"id"`
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type miRespInfo struct {
	ID      uint32   `json:"id"`
	Result  RespInfo `json:"result"`
	ExeTime uint32   `json:"exe_time"`
}

func newQueryInfo(id uint32) miQueryInfo {
	return miQueryInfo{ID: id, Method: "miIO.info", Params: make([]string, 0)}
}

// AddDeviceRequest is a pair of unverified IP Address and Token strings.
type AddDeviceRequest struct {
	IPAddr string
	Token  string
}

type Info struct {
	RespInfo
	DeviceID  wire.DeviceID
	Timestamp wire.Timestamp
}

type RespInfo struct {
	Life                uint32    `json:"life"`
	Model               string    `json:"model"`
	UID                 uint64    `json:"uid"`
	Token               string    `json:"token"`
	IpFlag              uint32    `json:"ipflag"`
	FirmwareVersion     string    `json:"fw_ver"`
	McuFirmwareVersion  string    `json:"mcu_fw_ver"`
	MiioVerion          string    `json:"miio_ver"`
	HwVersion           string    `json:"hw_ver"`
	MmFree              uint64    `json:"mmfree"`
	Mac                 string    `json:"mac"`
	WifiFirmwareVersion string    `json:"wifi_fw_ver"`
	Ap                  InfoAP    `json:"ap"`
	NetIf               InfoNetIf `json:"netif"`
	PowerMode           uint32    `json:"power_mode"`
}

type InfoAP struct {
	SSID    string `json:"ssid"`
	BSSID   string `json:"bssid"`
	RSSI    int8   `json:"rssi"`
	Primary int64  `json:"primary"`
}

type InfoNetIf struct {
	LocalIp netip.Addr `json:"localip"`
	Mask    netip.Addr `json:"mask"`
	Gateway netip.Addr `json:"gw"`
}

// ResolveFromIPToken tries to contact a device given an
// address and token.
//
// This method is lower-level than [ResolveDevice],
// is exported for use by utilities but
// should not generally be called.
func ResolveFromIPToken(
	ctx context.Context, addr netip.Addr,
	token *wire.Token, logger *slog.Logger,
) (Info, error) {
	// first, ping
	dialer := new(net.Dialer)
	addrPort := netip.AddrPortFrom(addr, wire.MiPort)
	pong, err := ping(ctx, dialer, addrPort, logger)
	if err != nil {
		return Info{}, err
	}

	// calculate epoch
	epoch := pong.Timestamp.EpochTime(time.Now())
	timestamp, err := wire.NewTimestamp(epoch, time.Now())
	if err != nil {
		return Info{}, err
	}

	info, err := dig(ctx, digArgs{
		deviceId:  pong.DeviceID,
		dialer:    dialer,
		addr:      addrPort,
		timestamp: timestamp,
		token:     token,
	})
	if err != nil {
		return Info{}, errors.Join(ErrDeviceDig, err)
	}
	return info, err
}

type digArgs struct {
	deviceId  wire.DeviceID
	dialer    *net.Dialer
	addr      netip.AddrPort
	timestamp wire.Timestamp
	token     *wire.Token
}

func dig(ctx context.Context, args digArgs) (Info, error) {
	connId := rand.Uint32()
	did := args.deviceId
	addr := args.addr
	timestamp := args.timestamp
	query := newQueryInfo(connId)
	token := args.token
	dialer := args.dialer

	var payload bytes.Buffer
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return Info{}, err
	}
	payload.Write(jsonBytes)
	payload.WriteByte(0)

	msg := wire.MiotPacket{
		DeviceID:  did,
		Timestamp: timestamp,
		Payload:   payload.Bytes(),
	}

	packetSend, err := token.Marshal(&msg)
	if err != nil {
		return Info{}, err
	}

	conn, err := dialer.DialUDP(ctx, "udp", netip.AddrPort{}, addr)
	if err != nil {
		return Info{}, err
	}
	defer conn.Close()

	_, err = conn.Write(packetSend)
	if err != nil {
		return Info{}, err
	}

	var buf [wire.MaxPayloadSize]byte
	n, err := conn.Read(buf[:])
	if err != nil {
		return Info{}, err
	}

	packetRecv, err := token.Unmarshal(buf[0:n])
	if err != nil {
		return Info{}, err
	}

	infoResp := new(miRespInfo)
	err = json.Unmarshal(packetRecv.Payload, infoResp)
	if err != nil {
		slog.Debug("dig unmarshal fail", "reason", err, "payload", string(packetRecv.Payload))
		return Info{}, err
	}

	return Info{
		RespInfo:  infoResp.Result,
		DeviceID:  did,
		Timestamp: timestamp,
	}, nil
}

// ResolveDefaultMetaspec finds a default Metaspec for a model name by
// running a comparator function given by cmp.
//
// The recommended logic is prioritizing Version over Timestamp.
func ResolveDefaultMetaspec(
	modelName string,
	metaspecs iter.Seq[config.Metaspec],
	cmp func(config.Metaspec, config.Metaspec) int,
) (*config.Metaspec, error) {
	// filter for matching model names first
	filter := func(yield func(m config.Metaspec) bool) {
		for metaspec := range metaspecs {
			if metaspec.Status == StatusReleased && metaspec.Model == modelName {
				if !yield(metaspec) {
					return
				}
			}
		}
	}
	metas := slices.Collect(filter)
	slices.SortFunc(metas, cmp)
	if len(metas) == 0 {
		return nil, fmt.Errorf("%w: %v", ErrNoMetaspec, modelName)
	} else {
		return &metas[len(metas)-1], nil
	}
}

func ping(ctx context.Context, d *net.Dialer, addr netip.AddrPort, logger *slog.Logger) (*wire.Pong, error) {
	l := logger.With("addr", &addr)
	conn, err := d.DialUDP(ctx, "udp", netip.AddrPort{}, addr)
	if err != nil {
		return nil, errors.Join(ErrDeviceDial, err)
	}
	l.Debug("dial")
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetReadDeadline(deadline)
	}
	defer conn.Close()

	_, err = conn.Write(wire.PingPacket)
	if err != nil {
		return nil, errors.Join(ErrDeviceSend, err)
	}
	l.Debug("write")

	var buf [wire.MaxPayloadSize]byte
	n, _, err := conn.ReadFrom(buf[:])
	if err != nil {
		return nil, errors.Join(ErrDeviceRecv, err)
	}
	l.Debug("read")

	return wire.GetPong(buf[0:n])
}

func (dev *Device) Info(ctx context.Context) (Info, error) {
	if dev.timeStart == nil {
		pong, err := ping(ctx, &dev.dialer, dev.Addr, dev.l)
		if err != nil {
			return Info{}, err
		}

		timeStart := pong.Timestamp.EpochTime(time.Now())
		dev.timeStart = &timeStart
	}

	tsp, err := wire.NewTimestamp(*dev.timeStart, time.Now())
	if err != nil {
		return Info{}, err
	}

	info, err := dig(ctx, digArgs{
		deviceId:  dev.DeviceID,
		dialer:    &dev.dialer,
		addr:      dev.Addr,
		timestamp: tsp,
		token:     &dev.Token,
	})
	if err != nil {
		return Info{}, errors.Join(ErrDeviceDig, err)
	}
	return info, nil
}

// ResolveDevice returns information about a device
// given an unparsed IP address and token.
//
// See [ResolveFromIPToken] for a low-level equivalent.
func ResolveDevice(ctx context.Context, adr AddDeviceRequest, logger *slog.Logger) (ResolvedDevice, error) {
	var cfgDev config.Device

	addr, err := netip.ParseAddr(adr.IPAddr)
	if err != nil {
		return ResolvedDevice{}, errors.Join(ErrDeviceResolve, err)
	}

	var tokenBytes [16]byte
	tokenLen, err := hex.Decode(tokenBytes[:], []byte(adr.Token))
	if err != nil {
		return ResolvedDevice{}, errors.Join(ErrDeviceResolve, err)
	}
	if tokenLen != wire.TokenLen {
		return ResolvedDevice{}, fmt.Errorf("%w: wrong token len %v", ErrDeviceResolve, err)
	}
	token, err := wire.NewToken(tokenBytes)
	if err != nil {
		return ResolvedDevice{}, errors.Join(ErrDeviceResolve, err)
	}

	devInfo, err := ResolveFromIPToken(ctx, addr, token, logger)
	if err != nil {
		return ResolvedDevice{}, errors.Join(ErrDeviceResolve, err)
	}

	cfgDev.IPAddr = addr
	cfgDev.Model = devInfo.Model
	cfgDev.Token = adr.Token
	// invalid version, deal with this later
	cfgDev.Version = 0
	cfgDev.Enabled = true
	return ResolvedDevice{
		cfgDev: cfgDev,
		Info:   devInfo,
	}, nil
}
