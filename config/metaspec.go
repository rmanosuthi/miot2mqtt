package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

const MetaspecsUrl = "https://miot-spec.org/miot-spec-v2/instances?status=all"
const MetaspecsPath = "vendor/miot_instances.json"

// Metaspecs is the raw representation of an "instances file" which
// contains info about all device specs.
//
// This file is quite large (>6MB) and is not streamed;
// low memory devices may struggle with its parsing.
type Metaspecs struct {
	Instances []Metaspec
}

// Default fetches the metaspecs file from miot-spec.org if
// AllowExternalNetwork is enabled.
func (ms *Metaspecs) Default(pfx *os.Root, gc *Global, hint *NoHint) error {
	if !gc.General.AllowExternalNetwork {
		return ErrNoExtNet
	}
	resp, err := http.Get(MetaspecsUrl)
	if err != nil {
		return fmt.Errorf("fetch instances: %w", err)
	}

	defer resp.Body.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(buf.Bytes(), ms)
	if err != nil {
		return fmt.Errorf("fetch instances: %w", err)
	}

	return nil
}

func (ms *Metaspecs) Suffix(hint *NoHint) (string, error) {
	return MetaspecsPath, nil
}

func (ms *Metaspecs) MarshalFunc() ([]byte, error) {
	return json.Marshal(ms)
}

func (ms *Metaspecs) UnmarshalFunc(src []byte) error {
	return json.Unmarshal(src, ms)
}

// Metaspec is a single entry of what's called an "Instance" in miot parlance
// but has been renamed to prevent confusion and better reflect what it is:
// metadata of a device specification.
type Metaspec struct {
	Status    string `json:"status"`
	Model     string `json:"model"`
	Version   uint64 `json:"version"`
	SpecURN   URN    `json:"type"`
	Timestamp uint64 `json:"ts"`
}
