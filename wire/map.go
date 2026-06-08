package wire

import (
	"errors"
	"log/slog"

	"golang.org/x/exp/constraints"
)

var ErrTypeConv = errors.New("type conversion")

// ValueMap translates values between
// a miot device and HA.
//
// It is sometimes necessary to do so since
// their types and values may not match up exactly.
//
// Example: an air purifier may have a fan-on speed range of 0-2
// (note 0 is not off, but rather what would be "speed 1" on most devices)
// but HA does not support having 0 be the minimum speed.
//
// HA would need to see 1-3 and the air purifier 0-2.
// An [IntOffsetMap] which implements this interface would do so.
type ValueMap interface {
	MiotHA(any) (any, error)
	HAMiot(any) (any, error)
}

// IdentityValueMap passes values through unmodified.
type IdentityValueMap struct{}

func (im *IdentityValueMap) MiotHA(src any) (any, error) {
	return src, nil
}

func (im *IdentityValueMap) HAMiot(src any) (any, error) {
	return src, nil
}

// IntOffsetMap applies an offset to an integer value.
//
// Example: an air purifier may have a fan-on speed range of 0-2
// (note 0 is not off, but rather what would be "speed 1" on most devices)
// but HA does not support having 0 be the minimum speed.
//
// HA would need to see 1-3 and the air purifier 0-2.
// An [IntOffsetMap] of value -1 would do so.
type IntOffsetMap[T constraints.Integer] int64

func (im *IntOffsetMap[T]) MiotHA(src any) (any, error) {
	isT, ok := src.(T)
	if !ok {
		return nil, ErrTypeConv
	}

	// TODO check correctness when src is uint64
	tmpVal := int64(isT)
	slog.Debug("int offset map", "direction", "miot -> ha", "offset", *im, "src", tmpVal)
	tmpVal -= int64(*im)

	var res T
	res = T(tmpVal)
	return res, nil
}

func (im *IntOffsetMap[T]) HAMiot(src any) (any, error) {
	isT, ok := src.(T)
	if !ok {
		return nil, ErrTypeConv
	}

	// TODO check correctness when src is uint64
	tmpVal := int64(isT)
	slog.Debug("int offset map", "direction", "ha -> miot", "offset", *im, "src", tmpVal)
	tmpVal += int64(*im)

	var res T
	res = T(tmpVal)
	return res, nil
}
