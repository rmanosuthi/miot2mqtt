package miot

import (
	"fmt"
	"strings"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
)

type LoadDevicesStage int

const (
	LoadDevicesStagePopulate LoadDevicesStage = iota
	LoadDevicesStageCount
	LoadDevicesStageMerge
	LoadDevicesStageParse
	LoadDevicesStageSpecsExist
	LoadDevicesStageSpecsDeferred
	LoadDevicesStageInit
)

type ErrLoadDevices struct {
	Stage  LoadDevicesStage
	Reason error
}

func (e *ErrLoadDevices) Error() string {
	var stage string
	switch e.Stage {
	case LoadDevicesStagePopulate:
		stage = "populate"
	case LoadDevicesStageCount:
		stage = "count"
	case LoadDevicesStageMerge:
		stage = "merge"
	case LoadDevicesStageParse:
		stage = "parse"
	case LoadDevicesStageSpecsExist:
		stage = "specs exist"
	case LoadDevicesStageSpecsDeferred:
		stage = "specs deferred"
	case LoadDevicesStageInit:
		stage = "init"
	}
	return fmt.Sprintf("load devices: %v: %v", stage, e.Reason.Error())
}

func (e *ErrLoadDevices) Unwrap() error {
	return e.Reason
}

type ErrDeviceMerge struct {
	DeviceID string
	New      config.Device
	Existing config.Device
}

func (e *ErrDeviceMerge) Error() string {
	return fmt.Sprintf("DeviceID %v already exists:\n%#v\nbut tried to add:\n%#v\n", e.DeviceID, e.Existing, e.New)
}

type ErrNoMetaspec string

func (e ErrNoMetaspec) Error() string {
	return fmt.Sprintf("no metaspec for model %v with selected parameters")
}

type ErrDevicesToAdd struct {
	Requests AddDeviceRequests
	Reason   error
}

func (e *ErrDevicesToAdd) Error() string {
	var sb strings.Builder
	sb.WriteString("devices to add:")
	for _, req := range e.Requests {
		sb.WriteRune(' ')
		sb.WriteString(req.IPAddr)
	}
	return sb.String()
}

func (e *ErrDevicesToAdd) Unwrap() error {
	return e.Reason
}

type ErrGetProps struct {
	Request prop.GetPropsReq
	Reason  error
}

func (e *ErrGetProps) Error() string {
	var sb strings.Builder
	sb.WriteString("get properties")
	for k, p := range e.Request {
		sb.WriteRune(' ')
		sb.WriteString(p.String(&k))
	}
	sb.WriteString(": ")
	sb.WriteString(e.Reason.Error())
	return sb.String()
}

func (e *ErrGetProps) Unwrap() error {
	return e.Reason
}

type ErrSetProps struct {
	Request prop.SetPropsReq
	Reason  error
}

func (e *ErrSetProps) Error() string {
	var sb strings.Builder
	sb.WriteString("set properties")
	for k, p := range e.Request {
		sb.WriteRune(' ')
		sb.WriteString(p.String(&k))
	}
	sb.WriteString(": ")
	sb.WriteString(e.Reason.Error())
	return sb.String()
}

func (e *ErrSetProps) Unwrap() error {
	return e.Reason
}
