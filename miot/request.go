// # Device Properties
//
// A "property" as defined here is not a single implementation
// but rather has a couple of concepts:
//
//   - "Alias" is its user-facing name.
//
//   - "Urn" is its globally-unique identifier stored as a map key in
//     [Device] to avoid collisions that would've been caused by
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
//     This is stored as a map value in [Device].
//
//     See [prop.PropKey] and [prop.SetPropKey].
//
//   - "Spec" is a device's spec's service's properties.
//
//     See [config.SpecService.Properties].
package miot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/netip"
	"time"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

// GetProperties sends a query to the device to get filtered properties' values.
// A predicate is accepted to only filter for properties of interest.
// Be mindful of the query size as some devices will return an error when
// a query is too big.
//
// Response types can be assumed to have been checked, but the caller may still want to typecast them.
//
// Cancel ctx to abort the request.
func (dev *Device) GetProperties(ctx context.Context, predicate func(config.URN, prop.PropKey) bool) (prop.GetPropsReq, error) {
	req := make(prop.GetPropsReq)
	for urn, propKey := range dev.PropKeys {
		if predicate(urn, propKey) {
			dev.l.Debug("GetProperties", "urn", urn)
			req[propKey] = &prop.GetProp{}
		}
	}

	err := dev.getProperties(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetProperties: %w", err)
	}

	return req, nil
}

// getProperties is a low-level helper to
// get a device's properties,
// mutating req in the process.
func (dev *Device) getProperties(ctx context.Context, req prop.GetPropsReq) error {
	if dev.timeStart == nil {
		dev.l.Debug("device uninit")
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

	if packetRecv.Timestamp < timestamp {
		dev.l.Warn("timestamp went backwards!", "prev", timestamp, "curr", packetRecv.Timestamp)
		return dev.UpdateTimestamp(packetRecv.Timestamp)
	}

	rprops, err := prop.ParseResponse(packetRecv.Payload)
	if err != nil {
		return err
	}

	for key, prop := range req {
		spec, ok := dev.Props[key]
		if !ok {
			return errors.Join(ErrDeviceRecv, fmt.Errorf("type mismatch"))
		}
		resp, err := key.Unwrap(spec, rprops)
		if err != nil {
			return errors.Join(ErrDeviceRecv, err)
		}
		prop.Response = resp
	}
	return nil
}

// SetProperty sends a single query to the device to
// set a single property.
func (dev *Device) SetProperty(ctx context.Context, key prop.PropKey, req *prop.SetProp) error {
	return dev.setProperties(ctx, prop.SetPropsReq{
		key: req,
	})
}

// setProperties is a low-level helper to
// set a device's properties,
// mutating req in the process.
func (dev *Device) setProperties(ctx context.Context, req prop.SetPropsReq) error {
	if dev.timeStart == nil {
		dev.l.Debug("device uninit")
		return ErrDeviceUninit
	}

	connId := rand.Uint32()
	query, err := prop.MakeSetQuery(connId, maps.All(req))
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

	if packetRecv.Timestamp < timestamp {
		dev.l.Warn("timestamp went backwards!", "prev", timestamp, "curr", packetRecv.Timestamp)
		return dev.UpdateTimestamp(packetRecv.Timestamp)
	}

	rprops, err := prop.ParseResponse(packetRecv.Payload)
	if err != nil {
		return err
	}

	for key, setProp := range req {
		resp, err := key.Unwrap(dev.Props[key], rprops)
		if err != nil {
			return errors.Join(ErrDeviceRecv, err)
		}
		setProp.Response = resp
	}

	return nil
}
