package wire

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const (
	MiTypeBool MiType = iota
	MiTypeUint8
	MiTypeUint16
	MiTypeUint32
	MiTypeInt8
	MiTypeInt16
	MiTypeInt32
	MiTypeInt64
	MiTypeFloat
	MiTypeString
	MiTypeHex
)

// MiType is a type marker found in spec files
// encoded as a JSON string field,
// with the content being the type's name.
// MiType does not contain the associated value.
//
// Types generally align with Go type names.
type MiType int

type MiValue struct {
	Type       MiType
	RawMessage json.RawMessage
	Value      any
}

// UnmarshalText parses a JSON string as a MiType.
func (t *MiType) UnmarshalText(text []byte) error {
	ty := string(text)
	switch ty {
	case "bool":
		*t = MiTypeBool
		return nil
	case "uint8":
		*t = MiTypeUint8
		return nil
	case "uint16":
		*t = MiTypeUint16
		return nil
	case "uint32":
		*t = MiTypeUint32
		return nil
	case "int8":
		*t = MiTypeInt8
		return nil
	case "int16":
		*t = MiTypeInt16
		return nil
	case "int32":
		*t = MiTypeInt32
		return nil
	case "int64":
		*t = MiTypeInt64
		return nil
	case "float":
		*t = MiTypeFloat
		return nil
	case "string":
		*t = MiTypeString
		return nil
	case "hex":
		*t = MiTypeHex
		return nil
	}
	return fmt.Errorf("unrecognized type %v", ty)
}

// MarshalText marshals the MiType as a JSON string.
func (t *MiType) MarshalText() ([]byte, error) {
	var name string
	switch *t {
	case MiTypeBool:
		name = "bool"
	case MiTypeUint8:
		name = "uint8"
	case MiTypeUint16:
		name = "uint16"
	case MiTypeUint32:
		name = "uint32"
	case MiTypeInt8:
		name = "int8"
	case MiTypeInt16:
		name = "int16"
	case MiTypeInt32:
		name = "int32"
	case MiTypeInt64:
		name = "int64"
	case MiTypeFloat:
		name = "float"
	case MiTypeString:
		name = "string"
	case MiTypeHex:
		name = "hex"
	default:
		return nil, fmt.Errorf("unknown type %v", *t)
	}
	return []byte(name), nil
}

// Convert takes what is assumed to be a JSON field encoded as bytes and:
//
//  1. Unmarshals it into the expected MiType
//  2. Converts the value using valueMap
//  3. Returns both the converted value and its marshaled form
func (mt *MiType) Convert(msg []byte, valueMap func(any) (any, error)) (MiValue, error) {
	if valueMap == nil {
		return MiValue{}, fmt.Errorf("valueMap func cannot be nil")
	}

	tmpVal, err := mt.cast(msg)
	if err != nil {
		return MiValue{}, err
	}

	val, err := valueMap(tmpVal)
	if err != nil {
		return MiValue{}, err
	}

	valBytes, err := json.Marshal(val)
	if err != nil {
		return MiValue{}, err
	}

	return MiValue{
		Type:       *mt,
		RawMessage: json.RawMessage(valBytes),
		Value:      val,
	}, nil
}

func (mt *MiType) cast(msg []byte) (any, error) {
	switch *mt {
	case MiTypeBool:
		var res bool
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeUint8:
		var res uint8
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeUint16:
		var res uint16
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeUint32:
		var res uint32
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeInt8:
		var res int8
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeInt16:
		var res int16
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeInt32:
		var res int32
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeInt64:
		var res int64
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeFloat:
		var res float32
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeString:
		var res string
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, err
		} else {
			return res, nil
		}
	case MiTypeHex:
		var tmp string
		err := json.Unmarshal(msg, &tmp)
		if err != nil {
			if res, err := hex.DecodeString(tmp); err != nil {
				return res, nil
			}
		}
		return tmp, err
	default:
		return "", fmt.Errorf("mitype fallthrough")
	}
}
