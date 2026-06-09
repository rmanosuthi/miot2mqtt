// # Communicating with devices
//
//  1. Start with a JSON
//
//  2. Append '\x00'
//
//  3. Pad with PKCS7
//
//  4. This is the "payload". Encrypt it.
//
//  5. Prepare header fields
//
//  6. Hash header, token, encrypted payload
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"
)

// TokenLen defines the length of a token.
const TokenLen = 16

// LenHeader defines a packet header's length.
const LenHeader = 32

// PadBlockSize defines the block size for PKCS7 padding.
const PadBlockSize = 16

// Devices listen on port 54321.
// Local communication back to us also comes from that port.
const MiPort = 54321

// magicRef is expected by devices to mark the start of a packet.
var magicRef = []byte{0x21, 0x31}

// PingPacket is a constant byte array used to ping a device.
// The only well-defined values are the magic and packet length (32).
var PingPacket = []byte{0x21, 0x31, 0x0, 0x20, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

var ErrPacketTooShort = errors.New("packet too short")
var ErrMagic = errors.New("packet magic mismatch")
var ErrCrypto = errors.New("crypto error")
var ErrTokenLen = errors.New("wrong token length")
var ErrDecode = errors.New("failed binary decode")
var ErrHashMismatch = errors.New("hash mismatch")

// DeviceID is a 32-bit unsigned integer identifying a device.
// It is used when assembling a packet.
type DeviceID uint32

func (did DeviceID) String() string {
	return strconv.FormatUint(
		uint64(did),
		10,
	)
}

// Timestamp is a 32-bit unsigned integer with its epoch being
// whenever the device was last reset or turned on.
//
// Resets may happen often when a device can't reach the cloud.
type Timestamp uint32

// EpochTime returns when the epoch was from the device's perspective.
//
// The result can be used in NewTimestamp.
func (ts Timestamp) EpochTime(now time.Time) time.Time {
	offset := time.Duration(ts)
	return now.Add(time.Second * -offset)
}

// NewTimestamp calculates a timestamp given an epoch and now.
func NewTimestamp(epoch time.Time, now time.Time) (Timestamp, error) {
	diffDuration := now.Sub(epoch) / time.Second
	if diffDuration > math.MaxUint32 {
		return 0, fmt.Errorf("timestamp overflow: %v", diffDuration)
	}
	return Timestamp(uint32(diffDuration)), nil
}

type Pong struct {
	DeviceID  DeviceID
	Timestamp Timestamp
}

// GetPong tries to parse a raw packet as Pong, the result of Ping.
// It contains a device ID and a timestamp.
func GetPong(rawPacket []byte) (Pong, error) {
	slog.Debug("getting pong")
	if _, err := atLeastHeader(rawPacket); err != nil {
		return Pong{}, fmt.Errorf("", err)
	}

	did, err := decode4(rawPacket[8:12])
	if err != nil {
		return Pong{}, err
	}
	timestamp, err := decode4(rawPacket[12:16])
	if err != nil {
		return Pong{}, err
	}
	return Pong{
		DeviceID:  DeviceID(did),
		Timestamp: Timestamp(timestamp),
	}, nil
}

// decode4 decodes a slice containing a big-endian uint32 into
// platform-native uint32.
func decode4(slice []byte) (uint32, error) {
	var res uint32
	_, err := binary.Decode(slice, binary.BigEndian, &res)
	if err != nil {
		return 0, err
	}
	return res, nil
}

// atLeastHeader asserts a packet is at least [LenHeader] long.
func atLeastHeader(rawPacket []byte) (int, error) {
	lenRaw := len(rawPacket)
	if lenRaw < LenHeader {
		return lenRaw, fmt.Errorf("packet too short: expected at least %v, found %v", LenHeader, lenRaw)
	}
	return lenRaw, nil
}

// GetDeviceID tries to get the device ID from a raw packet.
//
// It assumes the packet is at least LenHeader long.
func GetDeviceID(rawPacket []byte) (DeviceID, error) {
	if _, err := atLeastHeader(rawPacket); err != nil {
		return 0, err
	}

	val, err := decode4(rawPacket[8:12])
	if err != nil {
		return 0, err
	}
	return DeviceID(val), nil
}

// GetTimestamp tries to get the timestamp from a raw packet.
// It assumes the packet is at least [LenHeader] long.
func GetTimestamp(rawPacket []byte) (Timestamp, error) {
	if _, err := atLeastHeader(rawPacket); err != nil {
		return 0, err
	}

	val, err := decode4(rawPacket[12:16])
	if err != nil {
		return 0, err
	}
	return Timestamp(val), nil
}

// MiotPacket is the unmarshaled form of its wire format:
//
//	[0:2] magic 0x21 0x31
//	[2:4] header len (32) + payload len
//	[4:8] unknown
//	[8:12] device id
//	[12:16] timestamp
//	[16:32] checksum (MD5, yes really)
//	[32:] if present, encrypted payload
//
// Important: The payload is usually a JSON followed by `\0`,
// effectively a C string.
//
// No stripping or appending is done by this module.
type MiotPacket struct {
	DeviceID  DeviceID
	Timestamp Timestamp
	Payload   []byte
}
