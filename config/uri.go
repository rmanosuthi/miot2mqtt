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

type Urn struct {
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
	slog.Debug("segments", "val", segments)
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
	return nil
}

func (urn *Urn) MarshalText() ([]byte, error) {
	res := fmt.Sprintf("urn:%s:%s:%s:%x:%s:%d", urn.Namespace, urn.Type, urn.Name, urn.Value, urn.VendorProduct, urn.Version)
	return []byte(res), nil
}
