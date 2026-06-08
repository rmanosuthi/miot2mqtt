package config

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"unique"
)

var ErrUnmarshalUrn = errors.New("failed to unmarshal urn")

// URN is a non-globally-unique resource identifier
// used extensively by miot.
//
// URNs have a segment count of at least 7.
// Segments are divided by ":",
// with the first always being "urn" (discarded),
// and the second usually "miot-spec-v2".
type URN struct {
	// The full raw URN is stored here.
	raw unique.Handle[string]
	// Second segment.
	Namespace unique.Handle[string]
	// Third segment.
	Type unique.Handle[string]
	// Fourth segment.
	Name unique.Handle[string]
	// Fifth segment.
	Value uint32
	// Sixth segment.
	VendorProduct unique.Handle[string]
	// Seventh segment.
	Version uint64
}

// UnmarshalText parses a JSON string containing a URN.
// It expects at least 7 segments and also
// keeps the string as raw.
func (urn *URN) UnmarshalText(text []byte) error {
	raw := string(text)
	segments := strings.Split(raw, ":")
	lenSegs := len(segments)
	if lenSegs < 7 {
		return fmt.Errorf("%w: insufficient segment count %v", ErrUnmarshalUrn, lenSegs)
	}
	if lenSegs != 7 {
		slog.Debug("urn has unexpected segment count", "urn", raw)
	}

	if segments[0] != "urn" {
		return fmt.Errorf("%w: not urn", ErrUnmarshalUrn)
	}
	urn.Namespace = unique.Make(segments[1])
	urn.Type = unique.Make(segments[2])
	urn.Name = unique.Make(segments[3])
	valueBuf, _ := hex.DecodeString(segments[4])
	binary.Decode(valueBuf, binary.BigEndian, &urn.Value)
	urn.VendorProduct = unique.Make(segments[5])
	version, _ := strconv.ParseUint(segments[6], 10, 64)
	urn.Version = version
	urn.raw = unique.Make(raw)
	return nil
}

// MarshalText marshals the original raw URN into a JSON string.
func (urn *URN) MarshalText() ([]byte, error) {
	return []byte(urn.raw.Value()), nil
}

func (urn *URN) String() string {
	return urn.raw.Value()
}

func (urn URN) LogValue() slog.Value {
	return slog.StringValue(urn.raw.Value())
}
