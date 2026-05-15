package device

import (
	"bytes"
	"context"
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

var ErrDeviceDig = errors.New("failed to get device info")
var ErrNoMetaspec = errors.New("model has no metaspec")

type miQueryInfo struct {
	ID     uint32 `json:"id"`
	Method string `json:"method"`
	Params []byte `json:"params"`
}

type miRespInfo struct {
	ID      uint32   `json:"id"`
	Result  respInfo `json:"result"`
	ExeTime uint32   `json:"exe_time"`
}

func newQueryInfo(id uint32) miQueryInfo {
	return miQueryInfo{ID: id, Method: "miIO.info", Params: []byte{}}
}

type Info struct {
	respInfo
	DeviceID  wire.DeviceID
	Timestamp wire.Timestamp
}

type respInfo struct {
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

func ResolveFromIpToken(ctx context.Context, addr netip.Addr, token *wire.Token) (*Info, error) {
	// first, ping
	dialer := new(net.Dialer)
	addrPort := netip.AddrPortFrom(addr, wire.MiPort)
	pong, err := ping(ctx, dialer, addrPort)
	if err != nil {
		return nil, err
	}

	// calculate epoch
	epoch := pong.Timestamp.EpochTime(time.Now())
	timestamp, err := wire.NewTimestamp(epoch, time.Now())
	if err != nil {
		return nil, err
	}

	return dig(ctx, digArgs{
		deviceId:  pong.DeviceID,
		dialer:    dialer,
		addr:      addrPort,
		timestamp: timestamp,
		token:     token,
	})
}

type digArgs struct {
	deviceId  wire.DeviceID
	dialer    *net.Dialer
	addr      netip.AddrPort
	timestamp wire.Timestamp
	token     *wire.Token
}

func dig(ctx context.Context, args digArgs) (*Info, error) {
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
		return nil, errors.Join(ErrDeviceDig, err)
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
		return nil, errors.Join(ErrDeviceDial, err)
	}

	conn, err := dialer.DialUDP(ctx, "udp", netip.AddrPort{}, addr)
	if err != nil {
		return nil, errors.Join(ErrDeviceDial, err)
	}
	defer conn.Close()

	_, err = conn.Write(packetSend)
	if err != nil {
		return nil, errors.Join(ErrDeviceSend, err)
	}

	var buf [wire.MaxPayloadSize]byte
	n, err := conn.Read(buf[:])
	if err != nil {
		return nil, errors.Join(ErrDeviceRecv, err)
	}

	packetRecv, err := token.Unmarshal(buf[0:n])
	if err != nil {
		return nil, errors.Join(ErrDeviceRecv, err)
	}

	infoResp := new(miRespInfo)
	err = json.Unmarshal(packetRecv.Payload, infoResp)
	if err != nil {
		return nil, err
	}

	info := Info{
		respInfo:  infoResp.Result,
		DeviceID:  did,
		Timestamp: timestamp,
	}
	return &info, nil
}

// ResolveDefaultMetaspec finds a default Metaspec for a model name by
// running a comparator function given by cmp.
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

func ping(ctx context.Context, d *net.Dialer, addr netip.AddrPort) (*wire.Pong, error) {
	l := slog.Default().With("addr", &addr)
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
