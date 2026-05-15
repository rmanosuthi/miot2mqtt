package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
)

const MetaspecsUrl = "https://miot-spec.org/miot-spec-v2/instances?status=all"
const MetaspecsPath = "vendor/miot_instances.json"

var ErrNoExtNet = errors.New("external network access disabled")

type Metaspecs struct {
	Instances []Metaspec
}

func (ms *Metaspecs) Default(pfx *os.Root, gc *Global, hint *NoHint) error {
	if !gc.General.AllowExternalNetwork {
		return ErrNoExtNet
	}
	resp, err := http.Get(MetaspecsUrl)
	if err != nil {
		return fmt.Errorf("failed to get instances: %w", err)
	}

	defer resp.Body.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(buf.Bytes(), ms)
	if err != nil {
		return fmt.Errorf("failed to unmarshal instances: %w", err)
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

// Metaspec is called "Instance" in miot parlance
// but has been renamed to prevent confusion and
// better reflect what it is:
// metadata of a device specification.
//
// Populate isn't appropriate since it's a member of Metaspecs.
type Metaspec struct {
	Status    string `json:"status"`
	Model     string `json:"model"`
	Version   uint64 `json:"version"`
	SpecURN   URN    `json:"type"`
	Timestamp uint64 `json:"ts"`
}
