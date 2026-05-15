package config

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unique"
)

var ErrUnmarshalUrn = errors.New("failed to unmarshal urn")

type Urn struct {
	raw           unique.Handle[string]
	Namespace     unique.Handle[string]
	Type          unique.Handle[string]
	Name          unique.Handle[string]
	Value         uint32
	VendorProduct unique.Handle[string]
	Version       uint64
}

func (urn *Urn) UnmarshalText(text []byte) error {
	raw := string(text)
	segments := strings.Split(raw, ":")
	lenSegs := len(segments)
	if lenSegs != 7 {
		return fmt.Errorf("%w: wrong segment count %v", ErrUnmarshalUrn, lenSegs)
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

func (urn *Urn) MarshalText() ([]byte, error) {
	return []byte(urn.raw.Value()), nil
}

func (urn *Urn) String() string {
	return urn.raw.Value()
}
