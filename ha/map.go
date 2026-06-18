package ha

import (
	"errors"

	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrTypeConv = errors.New("type conversion")

// TopicEntry is metadata associated with an MQTT topic.
type TopicEntry struct {
	PropKey prop.PropKey
	// ValueMap is guaranteed to not be nil.
	ValueMap wire.ValueMap
}

// TopicMap is a map from an MQTT topic to a URN and a ValueMap.
type TopicMap map[Topic]TopicEntry

// PropExpansion defines a [config.SpecProp]'s
//  1. Arbitrary attribute names and their content
//     to be marshaled in the discovery message.
//  2. A map method to translate between
//     the miot device and HA.
//     Limited to one per property for simplicity.
//     See [ValueMap] for more info.
type PropExpansion struct {
	Attributes map[string]any
	ValueMap   wire.ValueMap
}
