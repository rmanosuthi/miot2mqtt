package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/rmanosuthi/miot2mqtt/wire"
)

type SpecID = uint16

var ErrNoSpecHint = errors.New("tried to fetch spec with no hint")

const SpecUrl = "https://miot-spec.org/miot-spec-v2/instance?type="
const SpecPath = "vendor/spec/"

// A Spec is specification for a single device model.
type Spec struct {
	Type        URN           `json:"type"`
	Description string        `json:"description"`
	Services    []SpecService `json:"services"`
}

// SpecService is a service provided by a device model.
//
// Example: an air purifier may have services
// "air-purifier" for main controls,
// "environment" for temperature and PM2.5 reporting, and
// "filter" for filter status.
type SpecService struct {
	IID         SpecID       `json:"iid"`
	Type        URN          `json:"type"`
	Description string       `json:"description"`
	Properties  []SpecProp   `json:"properties,omitempty"`
	Actions     []SpecAction `json:"actions,omitempty"`
	Events      []SpecEvent  `json:"events,omitempty"`
}

func (s *SpecService) Name() string {
	return s.Type.Name.Value()
}

// SpecProp is a property provided by a device model
// which lives under a SpecService.
//
// Example: a fan may have properties
// "on" for on/off,
// "fan-level" for fan level, and
// "horizontal-swing" for horizontal swing.
type SpecProp struct {
	IID SpecID `json:"iid"`
	// Urn is renamed from "type" to better reflect what it is.
	Urn         URN         `json:"type"`
	Description string      `json:"description"`
	Format      wire.MiType `json:"format"`
	Access      []string    `json:"access"`
	// A continuous integer/floating point range, defined as
	// min, max, step.
	ValueRange []json.Number `json:"value-range,omitempty"`
	// A list of possible integer values.
	// May not be continuous.
	ValueList []SpecPropValue `json:"value-list,omitempty"`
	Unit      string          `json:"unit,omitempty"`
	//GattAccess []any           `json:"gatt-access,omitempty"`
	Source SpecID `json:"source,omitempty"`
	// string only
	MaxLength SpecID `json:"max-length"`
}

func (s *SpecProp) Name() string {
	return s.Urn.Name.Value()
}

func (s *SpecProp) Read() bool {
	return slices.Contains(s.Access, "read")
}

func (s *SpecProp) Write() bool {
	return slices.Contains(s.Access, "write")
}

func VList[T ~uint8 | ~uint16](s *SpecProp) iter.Seq2[T, SpecPropValue] {
	return func(yield func(T, SpecPropValue) bool) {
		for _, pv := range s.ValueList {
			iv, err := pv.Value.Int64()
			if err != nil {
				slog.Error("cannot convert spec value list element to int64", "val", pv.Value)
				return
			}
			conv := T(iv)
			if !yield(conv, pv) {
				return
			}
		}
	}
}

type SpecPropValue struct {
	Value       json.Number `json:"value"`
	Description string      `json:"description"`
}

type SpecAction struct {
	IID         SpecID `json:"iid"`
	Type        URN    `json:"type"`
	Description string `json:"description"`
	In          []any  `json:"in"`
	Out         []any  `json:"out"`
}

type SpecEvent struct {
	IID         SpecID        `json:"iid"`
	Type        URN           `json:"type"`
	Description string        `json:"description"`
	Arguments   []json.Number `json:"arguments"`
}

type specReq struct {
	Body      *bytes.Buffer
	BytesRead int64
	Error     error
}

type SpecDownload struct {
	URN     URN
	Context context.Context
}

// SpecHint is information given to
// populate a spec file.
type SpecHint struct {
	Model string
	// Version of the spec file. Can be determined from Metaspec.
	Version  uint64
	Download *SpecDownload
}

func (spec *Spec) Default(pfx *os.Root, gc *Global, hint *SpecHint) error {
	if hint.Version == 0 || hint.Download == nil {
		return ErrNoSpecHint
	}
	if !gc.General.AllowExternalNetwork {
		return ErrNoExtNet
	}
	slog.Info("downloading device spec", "model", hint.Model, "version", hint.Version)
	return downloadSpec(hint.Download.Context, hint.Download.URN, spec)
}

func (spec *Spec) Suffix(hint *SpecHint) (string, error) {
	var sb strings.Builder
	sb.WriteString(SpecPath)
	fmt.Fprintf(&sb, "v%v.", hint.Version)
	sb.WriteString(hint.Model)
	sb.WriteString(".json")
	return sb.String(), nil
}

func (spec *Spec) MarshalFunc() ([]byte, error) {
	return json.Marshal(spec)
}

func (spec *Spec) UnmarshalFunc(src []byte) error {
	err := json.Unmarshal(src, spec)
	if err != nil {
		slog.Debug("failed to unmarshal spec, dumping", "dump", spec)
	}
	return err
}

func downloadSpec(ctx context.Context, urn URN, spec *Spec) error {
	ch := make(chan specReq)
	url := SpecUrl + urn.String()
	go func(ctx context.Context) {
		slog.Debug("downloading device spec", "url", url)
		resp, err := http.Get(url)
		if err != nil {
			ch <- specReq{Body: nil, Error: err}
			return
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		br, err := buf.ReadFrom(resp.Body)
		if err != nil {
			ch <- specReq{Body: nil, Error: err}
			return
		}
		ch <- specReq{Body: &buf, BytesRead: br, Error: nil}
	}(ctx)
	res := <-ch
	if res.Error != nil {
		return res.Error
	}
	err := json.Unmarshal(res.Body.Bytes(), spec)
	return err
}
