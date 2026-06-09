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
//
//   - "Value map" is a transformation between a value the device expects
//     and a value from an external source.
//     The external source is usually from HA.
//
// # Request submission
//
// Get/set requests to a device can be sent by following these steps:
//
//  1. Get needed [prop.PropKey]s and associated [config.SpecProp]s from [Device].
//
//  2. [wire.ValueMap] will depend on the property.
//     Most should use [wire.IdentityValueMap].
//
//  3. The request will be of type [prop.GetPropsReq] or [prop.SetPropsReq],
//     both being a map from [prop.PropKey] to Get/SetProp.
//     Create the map and call [prop.NewGetProp] or [prop.NewSetProp]
//     for each property.
//     Both constructors require [config.SpecProp] and [wire.ValueMap],
//     and will mutate the request in place.
//
//  4. Submit the request using [GetProperties] or [SetProperties].
//
//  5. Iterate over the request. Each property's value will be in
//     .Response.Value; see [wire.MiValue].
package miot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/netip"
	"time"

	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

// GetProperties sends a query to the device to get
// requested properties.
// Be mindful of the query size as some devices will return an error when
// a query is too big.
// Cancel ctx to abort the request.
//
// See [Request Submission] for more info.
func (dev *Device) GetProperties(ctx context.Context, req prop.GetPropsReq) error {
	err := dev.getProperties(ctx, req)
	if err != nil {
		return &ErrGetProps{
			Request: req,
			Reason:  err,
		}
	}
	return nil
}

// getProperties is a low-level helper to
// get a device's properties,
// mutating req in the process.
func (dev *Device) getProperties(ctx context.Context, req prop.GetPropsReq) error {
	if dev.timeStart == nil {
		dev.l.Debug("device uninit")
		return fmt.Errorf("device not initialized")
	}

	connId := rand.Uint32()
	query, err := prop.MakeGetQuery(connId, req)
	if err != nil {
		return err
	}

	var payload bytes.Buffer
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return err
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
		return err
	}

	conn, err := dev.dialer.DialUDP(ctx, "udp", netip.AddrPort{}, dev.Addr)
	if err != nil {
		return err
	}
	go func() {
		if deadline, ok := ctx.Deadline(); ok {
			conn.SetDeadline(deadline)
		} else {
			<-ctx.Done()
			conn.Close()
		}
	}()
	defer conn.Close()

	_, err = conn.Write(packetSend)
	if err != nil {
		return err
	}

	var buf [wire.MaxPayloadSize]byte
	n, err := conn.Read(buf[:])
	if err != nil {
		return err
	}

	packetRecv, err := dev.Token.Unmarshal(buf[0:n])
	if err != nil {
		return err
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
		vm := prop.ValueMap(key)
		resp, err := key.Unwrap(rprops, vm)
		if err != nil {
			return err
		}
		prop.Response = resp
	}
	return nil
}

// SetProperty sends a single query to the device to
// set a single property.
func (dev *Device) SetProperty(ctx context.Context, key prop.PropKey, sp *prop.SetProp) error {
	req := prop.SetPropsReq{
		key: sp,
	}

	err := dev.setProperties(ctx, req)
	if err != nil {
		return &ErrSetProps{
			Request: req,
			Reason:  err,
		}
	}
	return nil
}

// setProperties is a low-level helper to
// set a device's properties,
// mutating req in the process.
func (dev *Device) setProperties(ctx context.Context, req prop.SetPropsReq) error {
	if dev.timeStart == nil {
		dev.l.Debug("device uninit")
		return fmt.Errorf("device not initialized")
	}

	connId := rand.Uint32()
	query, err := prop.MakeSetQuery(connId, maps.All(req))
	if err != nil {
		return err
	}

	var payload bytes.Buffer
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return err
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
		return err
	}

	conn, err := dev.dialer.DialUDP(ctx, "udp", netip.AddrPort{}, dev.Addr)
	if err != nil {
		return err
	}
	go func() {
		if deadline, ok := ctx.Deadline(); ok {
			conn.SetDeadline(deadline)
		} else {
			<-ctx.Done()
			conn.Close()
		}
	}()
	defer conn.Close()

	_, err = conn.Write(packetSend)
	if err != nil {
		return err
	}

	var buf [wire.MaxPayloadSize]byte
	n, err := conn.Read(buf[:])
	if err != nil {
		return err
	}

	packetRecv, err := dev.Token.Unmarshal(buf[0:n])
	if err != nil {
		return err
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
		vm := setProp.ValueMap(key)
		resp, err := key.Unwrap(rprops, vm)
		if err != nil {
			return err
		}
		setProp.Response = resp
	}

	return nil
}
