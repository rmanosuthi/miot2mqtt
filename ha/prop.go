package ha

import (
	"errors"
	"math"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"golang.org/x/exp/constraints"
)

var ErrNoMinMax = errors.New("property does not have min/max range")

type key = prop.PropKey
type spec = *config.SpecProp

// PropRewrite is the marshaled form of [RewriteEntry].
type PropRewrite struct {
	Match []byte
	//
	Target  string
	Content []byte
}

// PropDecl represents attributes that will be associated with a miot property.
type PropDecl struct {
	// Mandatory means a given device must have this property.
	// Initialization should fail otherwise.
	//
	// Example: a fan must always have the "on" property, but
	// "vertical-swing" is optional.
	Mandatory bool
	// Prefix gets prepended to generated attribute names.
	// An empty value means this route belongs to a component's main property.
	Prefix string
	// (Optional) TODO
	Rewrite PropRewrite
	// (Optional) Expand will be called to
	// generate [wire.ValueMap] and
	// append more attributes,
	// which don't have to be MQTT paths, to the result.
	// The spec will be that of the URN matched by its Name segment.
	//
	// See [PropExpansion] for a longer explanation.
	Expand func(spec) (PropExpansion, error)
}

// Attr returns the attribute fragment of a property.
func (pd *PropDecl) Attr() string {
	if pd.Prefix == "default" {
		return ""
	} else {
		return pd.Prefix + "_"
	}
}

// PropDecls is a collection of property declarations.
// The key is used for looking up a
// [config.SpecProp] from its URN name.
type PropDecls map[string]PropDecl

type Range[T constraints.Integer] struct {
	Min T
	Max T
}

// MinMax extracts the min and max value from a SpecProp.
// ValueRange is first checked, then ValueList,
// returning at the earliest field that satisfies the request.
//
// FIXME Temporary helper, does not support float.
func MinMax[T constraints.Integer](s *config.SpecProp) (Range[T], error) {
	var minVal int64 = math.MaxInt64
	var maxVal int64 = math.MinInt64

	if len(s.ValueList) >= 2 {
		for _, pv := range s.ValueList {
			v, err := pv.Value.Int64()
			if err != nil {
				return Range[T]{}, err
			}
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
		return Range[T]{
			Min: T(minVal),
			Max: T(maxVal),
		}, nil
	} else if len(s.ValueRange) >= 2 {
		for _, pv := range s.ValueRange {
			v, err := pv.Int64()
			if err != nil {
				return Range[T]{}, err
			}
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
		return Range[T]{
			Min: T(minVal),
			Max: T(maxVal),
		}, nil
	} else {
		return Range[T]{}, ErrNoMinMax
	}
}
