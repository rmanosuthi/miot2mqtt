// # Device Properties
//
// A "property" as defined here is not a single implementation
// but rather has a couple of concepts:
//
//   - "Alias" is its user-facing name.
//
//   - "Urn" is its globally-unique identifier stored as a map key in
//     [MiotDevice] to avoid collisions that would've been caused by
//     using an alias.
//
//     See [config.SpecProp.Type].
//
//   - "Type" is the returned value's type in get operations,
//     and the input value's type in set operations.
//
//   - "Key" is a type used to enforce type safety,
//     access the correct property in get/set operations, and
//     unwrap a set operation's return value.
//     This is stored as a map value in [MiotDevice].
//
//     See [prop.PropKey] and [prop.SetPropKey].
//
//   - "Spec" is a device's spec's service's properties.
//
//     See [config.SpecService.Properties].
package device

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/netip"
	"slices"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/device/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

// GetProperties sends a query to the device to get filtered properties' values.
// A predicate is accepted to only filter for properties of interest.
// Be mindful of the query size as some devices will return an error when
// a query is too big.
//
// Examine a property's spec by accessing [prop.PropKey.Ref].
//
// Cancel ctx to abort the request.
func (dev *MiotDevice) GetProperties(ctx context.Context, predicate func(config.Urn, prop.PropKey) bool) (prop.GetPropsReq, error) {
	req := make(prop.GetPropsReq)
	for urn, propKey := range dev.Props {
		if predicate(urn, propKey) {
			slog.Debug("GetProperties", "urn", urn)
			req[urn] = prop.GetProp{
				PropKey: propKey,
			}
		}
	}

	err := dev.getProperties(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetProperties")
	}

	return req, nil
}

func (dev *MiotDevice) getProperties(ctx context.Context, req prop.GetPropsReq) error {
	if dev.timeStart == nil {
		slog.Debug("device uninit", "device", dev.DeviceID, "addr", dev.Addr)
		return ErrDeviceUninit
	}

	connId := rand.Uint32()
	query, err := prop.MakeGetQuery(connId, req)
	if err != nil {
		return err
	}

	var payload bytes.Buffer
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return errors.Join(ErrDeviceDial, err)
	}
	payload.Write(jsonBytes)
	payload.WriteByte(0)

	timestamp, err := wire.NewTimestamp(*dev.timeStart, time.Now())
	msg := wire.MiotPacket{
		DeviceID:  dev.DeviceID,
		Timestamp: timestamp,
		Payload:   payload.Bytes(),
	}
	packetSend, err := dev.Token.Marshal(&msg)
	if err != nil {
		return errors.Join(ErrDeviceDial, err)
	}

	conn, err := dev.dialer.DialUDP(ctx, "udp", netip.AddrPort{}, dev.Addr)
	if err != nil {
		return errors.Join(ErrDeviceDial, err)
	}
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetReadDeadline(deadline)
		conn.SetWriteDeadline(deadline)
	}
	defer conn.Close()

	_, err = conn.Write(packetSend)
	if err != nil {
		return errors.Join(ErrDeviceSend, err)
	}

	var buf [wire.MaxPayloadSize]byte
	n, err := conn.Read(buf[:])
	if err != nil {
		return errors.Join(ErrDeviceRecv, err)
	}

	packetRecv, err := dev.Token.Unmarshal(buf[0:n])
	if err != nil {
		return errors.Join(ErrDeviceRecv, err)
	}

	rprops, err := prop.ParseResponse(packetRecv.Payload)
	if err != nil {
		return err
	}

	for _, prop := range req {
		key := prop.PropKey
		resp, err := key.Unwrap(rprops)
		if err != nil {
			return errors.Join(ErrDeviceRecv, err)
		}
		prop.Response = resp
	}
	return nil
}

func (dev *MiotDevice) SetProperty(ctx context.Context, req prop.SetProp) error {
	keys := []prop.SetProp{req}
	query, err := prop.MakeSetQuery(uint32(dev.DeviceID), slices.Values(keys))
	if err != nil {
		return err
	}

	rprops, err := dev.sendSetProps(ctx, query)
	if err != nil {
		return err
	}

	resp, err := req.Unwrap(rprops)
	if err != nil {
		return errors.Join(ErrDeviceRecv, err)
	}

	req.Response = resp
	return nil
}

func (dev *MiotDevice) sendSetProps(ctx context.Context, query prop.RawQuery) ([]prop.ResponseEntry, error) {
	var payload bytes.Buffer
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return nil, errors.Join(ErrDeviceDial, err)
	}
	payload.Write(jsonBytes)
	payload.WriteByte(0)

	timestamp, err := wire.NewTimestamp(*dev.timeStart, time.Now())
	msg := wire.MiotPacket{
		DeviceID:  dev.DeviceID,
		Timestamp: timestamp,
		Payload:   payload.Bytes(),
	}
	packetSend, err := dev.Token.Marshal(&msg)
	if err != nil {
		return nil, errors.Join(ErrDeviceDial, err)
	}

	conn, err := dev.dialer.DialUDP(ctx, "udp", netip.AddrPort{}, dev.Addr)
	if err != nil {
		return nil, errors.Join(ErrDeviceDial, err)
	}
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetReadDeadline(deadline)
		conn.SetWriteDeadline(deadline)
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

	packetRecv, err := dev.Token.Unmarshal(buf[0:n])
	if err != nil {
		return nil, errors.Join(ErrDeviceRecv, err)
	}

	rprops, err := prop.ParseResponse(packetRecv.Payload)
	if err != nil {
		return nil, err
	}

	return rprops, nil
}

func (dev *MiotDevice) SetProperties(ctx context.Context, req prop.SetPropsReq) error {
	if dev.timeStart == nil {
		slog.Debug("device uninit", "device", dev.DeviceID, "addr", dev.Addr)
		return ErrDeviceUninit
	}
	connId := rand.Uint32()
	query, err := prop.MakeSetQuery(connId, maps.Values(req))
	if err != nil {
		return err
	}

	rprops, err := dev.sendSetProps(ctx, query)
	if err != nil {
		return err
	}

	for _, key := range req {
		resp, err := key.Unwrap(rprops)
		if err != nil {
			return errors.Join(ErrDeviceRecv, err)
		}
		key.Response = resp
	}
	return nil
}
