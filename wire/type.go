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

type MiType int

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

func (mt *MiType) Cast(msg json.RawMessage) (any, bool) {
	switch *mt {
	case MiTypeBool:
		var res bool
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeUint8:
		var res uint8
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeUint16:
		var res uint16
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeUint32:
		var res uint32
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeInt8:
		var res int8
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeInt16:
		var res int16
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeInt32:
		var res int32
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeInt64:
		var res int64
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeFloat:
		var res float32
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeString:
		var res string
		err := json.Unmarshal(msg, &res)
		if err != nil {
			return res, false
		} else {
			return res, true
		}
	case MiTypeHex:
		var tmp string
		err := json.Unmarshal(msg, &tmp)
		if err != nil {
			if res, err := hex.DecodeString(tmp); err != nil {
				return res, true
			}
		}
		return tmp, false
	default:
		return "", false
	}
}
