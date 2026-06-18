package ha

// A Topic is a typed absolute path string
// for an MQTT topic.
// It is produced by chaining *Topic types'
// transformation functions together, such as
// [DeviceTopic.Component] and [ComponentTopic.Property].
type Topic string

// MQTT extracts the string of the Topic and
// must only be called by [MQTTHandle].
func (t *Topic) MQTT() string {
	return string(*t)
}

// DeviceTopic is a typed absolute path string
// for a device's MQTT topic.
// This is returned from [GetDeviceTopic] and
// is defined as:
//
//	miot2mqtt/{DeviceID}
type DeviceTopic string

// Glob returns the wildcard topic for a device.
// This should only be used for routing and never for subscribing
// to prevent send-receive loops.
func (dt DeviceTopic) Glob() string {
	return string(dt) + "/#"
}

// Component produces a component topic when given its template
// and a DeviceTopic.
func (dt DeviceTopic) Component(cmp ComponentTemplate) ComponentTopic {
	return ComponentTopic(string(dt) + "/" + cmp.Platform + "/" + cmp.Canon())
}

// ComponentTopic is a typed path string
// for a HA component's MQTT topic.
// This is returned from [DeviceTopic.Component] and
// is defined as:
//
//	miot2mqtt/{DeviceID}/{ComponentPlatform}/{ComponentCanon}
//
// See also: [Canon].
type ComponentTopic string

// AsRoot returns the absolute path of a ComponentTopic
// to be used as [ComponentDiscovery]'s
// root path.
func (ct ComponentTopic) AsRoot() string {
	return string(ct)
}

// AvailRel returns the relative path of the availability topic
// to be used by [ComponentDiscovery].
func (ct ComponentTopic) AvailRel() string {
	return "~/availability"
}

// AvailTopic returns the typed absolute path
// of the availability topic
// to be used for publishing status updates.
func (ct ComponentTopic) AvailTopic() Topic {
	return Topic(string(ct) + "/availability")
}

// Property produces a property topic given a
// property declaration and a component topic.
func (ct ComponentTopic) Property(decl *PropDecl) PropertyTopic {
	if decl.Prefix == "" {
		return PropertyTopic{abs: string(ct) + "/default", rel: "~/default"}
	} else {
		return PropertyTopic{abs: string(ct) + "/" + decl.Prefix, rel: "~/" + decl.Prefix}
	}
}

// PropertyTopic is a typed string encoding both
// absolute and relative paths
// for a HA component property's MQTT topic.
//
// When the property has no Prefix, this is
//
//	miot2mqtt/{DeviceID}/{ComponentPlatform}/{ComponentCanon}/default
//
// Otherwise,
//
//	miot2mqtt/{DeviceID}/{ComponentPlatform}/{ComponentCanon}/{Prefix}
//
// The relative form can be obtained by passing abs = false.
type PropertyTopic struct {
	abs string
	rel string
}

func (pt PropertyTopic) Command(abs bool) Topic {
	if abs {
		return Topic(pt.abs + "/command")
	} else {
		return Topic(pt.rel + "/command")
	}
}

func (pt PropertyTopic) State(abs bool) Topic {
	if abs {
		return Topic(pt.abs + "/state")
	} else {
		return Topic(pt.rel + "/state")
	}
}
