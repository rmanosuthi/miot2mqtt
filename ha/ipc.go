// # IPC
//
// Names follow the convention FromToName.
// Example: MqDpConnected = from MQ, to DevicePool, name Connected.
//
// MQ to DevicePool
//
//   - [MqDpConnected]
//   - [MqDpReqDiscovery]
//
// DevicePool to MQ
//
//   - [DpMqConnInfo]
//
// DevicePool to Device
//
//   - [DpDevReqDiscovery]
//
// Device to MQ
//
//   - [DevMqPost] through DevicePool
package ha

import (
	"encoding/json"
	"errors"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrChFull = errors.New("channel is full")

// MqDpConnected is sent from MQTT to DevicePool to
// request subscription and routing info for devices.
//
// It expects responses to be submitted to
// ReplyTo.
type MqDpConnected struct {
	ReplyTo chan<- DpMqConnInfo
}

// MqDpReqDiscovery is sent from MQTT to DevicePool to
// request that devices generate their discovery messages.
type MqDpReqDiscovery struct {
	Conn *autopaho.ConnectionManager
}

// MqDevPublish is sent directly from MQTT to Device, using
// the routing handler, to notify that a new message
// has arrived.
//
// This message is special in that communication bypasses DevicePool;
// see [NewDevice] and [DpMqConnInfo.ForwardTo].
type MqDevPublish struct {
	Topic   Topic
	Payload []byte
}

// DpMqConnInfo is sent from DevicePool to MQTT,
// prepared once on device creation,
// as a response to [MqDpConnected].
type DpMqConnInfo struct {
	// Device ID.
	DID wire.DeviceID
	// Subscription topics.
	SubTopics  TopicMap
	DeviceMbox chan<- any
}

type DpDevReqDiscovery MqDpReqDiscovery

// DevMqPost is a generic message.
type DevMqPost struct {
	DID     wire.DeviceID
	Payload map[Topic]json.RawMessage
}
