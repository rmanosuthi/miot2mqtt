package wire

import (
	"fmt"
	"reflect"
)

type MiType struct {
	reflect.Type
}

func (t *MiType) UnmarshalText(text []byte) error {
	ty := string(text)
	switch ty {
	case "bool":
		t.Type = reflect.TypeFor[bool]()
		return nil
	case "uint8":
		t.Type = reflect.TypeFor[uint8]()
		return nil
	case "uint16":
		t.Type = reflect.TypeFor[uint16]()
		return nil
	case "uint32":
		t.Type = reflect.TypeFor[uint32]()
		return nil
	case "int8":
		t.Type = reflect.TypeFor[int8]()
		return nil
	case "int16":
		t.Type = reflect.TypeFor[int16]()
		return nil
	case "int32":
		t.Type = reflect.TypeFor[int32]()
		return nil
	case "int64":
		t.Type = reflect.TypeFor[int64]()
		return nil
	// vendor spec file may not specify float size
	// default to 32 and save it as "float32"
	case "float":
		t.Type = reflect.TypeFor[float32]()
		return nil
	case "float32":
		t.Type = reflect.TypeFor[float32]()
		return nil
	case "float64":
		t.Type = reflect.TypeFor[float64]()
		return nil
	case "string":
		t.Type = reflect.TypeFor[string]()
		return nil
	case "hex":
		// TODO
		t.Type = reflect.TypeFor[string]()
		return nil
	}
	return fmt.Errorf("unrecognized type %v", ty)
}

func (t *MiType) MarshalText() ([]byte, error) {
	return []byte(t.Name()), nil
}
