package ha

// DeviceTopic is a typed absolute path string
// for a device's MQTT topic.
// This is returned from [Resolver.GetDeviceTopic] and
// is defined as:
//
//	miot2mqtt/{DeviceID}
type DeviceTopic string

// ComponentTopic is a typed absolute path string
// for a HA component's MQTT topic.
// This is returned from [DeviceTopic.Component] and
// is defined as:
//
//	miot2mqtt/{DeviceID}/{ComponentPlatform}/{ComponentCanon}
//
// See also: [Canon].
type ComponentTopic string

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
// The relative form can be obtained through its methods.
type PropertyTopic struct {
	abs string
	rel string
}

func (dt DeviceTopic) Glob() string {
	return string(dt) + "/#"
}

func (dt DeviceTopic) Component(cmp Component) ComponentTopic {
	return ComponentTopic(string(dt) + "/" + cmp.Platform + "/" + Canon(cmp))
}

func (ct ComponentTopic) AsRoot() string {
	return string(ct)
}

func (ct ComponentTopic) AvailRel() string {
	return "~/availability"
}

func (ct ComponentTopic) AvailTopic() Topic {
	return Topic(string(ct) + "/availability")
}

func (ct ComponentTopic) Property(decl *PropDecl) PropertyTopic {
	if decl.Prefix == "" {
		return PropertyTopic{abs: string(ct) + "/default", rel: "~/default"}
	} else {
		return PropertyTopic{abs: string(ct) + "/" + decl.Prefix, rel: "~/" + decl.Prefix}
	}
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
