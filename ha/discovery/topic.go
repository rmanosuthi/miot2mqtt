package discovery

type DeviceTopic string
type ComponentTopic string
type PropertyTopic struct {
	abs string
	rel string
}

func (dt DeviceTopic) Component(cmp Component) ComponentTopic {
	return ComponentTopic("/" + string(dt) + "/" + cmp.Platform() + "/" + Canon(cmp))
}

func (ct ComponentTopic) AsRoot() string {
	return string(ct)
}

func (ct ComponentTopic) Property(decl *PropDecl) PropertyTopic {
	if decl.Prefix == "" {
		return PropertyTopic{abs: string(ct) + "/default", rel: "~/default"}
	} else {
		return PropertyTopic{abs: string(ct) + "/" + decl.Prefix, rel: "~/" + decl.Prefix}
	}
}

func (pt PropertyTopic) Command(abs bool) string {
	if abs {
		return pt.abs + "/command"
	} else {
		return pt.rel + "/command"
	}
}

func (pt PropertyTopic) State(abs bool) string {
	if abs {
		return pt.abs + "/state"
	} else {
		return pt.rel + "/state"
	}
}
