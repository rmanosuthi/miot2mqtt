package discovery

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"strings"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrComponentAttach = errors.New("failed to attach component")

// FIXME make more robust
var rgCanon = regexp.MustCompile(`[\s\-]+`)

const BasePath = "miot2mqtt"

// A ComponentHandle is an in-memory representation of
// a Component associated with a device.
type ComponentHandle struct {
	// A reference to the Component is kept just in case.
	cmp Component
	// Device ID of the associated [miot.Device].
	did wire.DeviceID
	// HA platform of the component. Examples: "fan", "number", "switch"
	platform      string
	CommandTopics TopicMap
	StateTopics   TopicMap
	Discovery     ComponentDiscovery
	Canon         string
	AvailTopic    Topic
}

// A Component is a controllable single-platform entity
// belonging to a [Device].
// It is used to set up routes and discovery messages.
//
// Example: a fan may have several Components:
//
//   - Platform Fan: on/off, fan speed, horizontal oscillation
//   - Platform Number: horizontal swing angle
//   - Platform Number: vertical swing angle
//   - Platform Switch: vertical oscillation
//
// # Attributes
//
// We will call JSON map key-value pairs in a discovery message's component
// "attributes". Example:
//
//	[...],
//	"cmps": {
//	  "some_unique_component_id1": {
//	    "p": "sensor",
//	    "device_class":"temperature",
//	    "unit_of_measurement":"°C",
//	    "value_template":"{{ value_json.temperature }}",
//	    "unique_id":"temp01ae_t"
//	  },
//	}
//
// The component identified by "some_unique_component_id1" has attributes
// "p", "device_class", "unit_of_measurement",
// "value_template", and "unique_id".
//
// # Identification
//
// Home Assistant UI component label:
//
//	{Alias}
//
// Discovery message:
//
//	component name in device discovery's components map: {Canon}
//	unique_id: miot2mqtt_{DeviceID}_{Platform}_{Canon}
//	name: {Alias}
//
// # Availability
//
// The topic is declared as ~/availability.
//
// A component is simply marked as online whenever Device starts and
// offline when the program exits.
// This will most likely change in the future.
type Component struct {
	// Mandatory tells the resolver this component's initialization must succeed,
	// else the entire device's initialization will be aborted.
	Mandatory bool
	// Alias is the user-facing name of the component.
	Alias string
	// Platform is an interaction UI group on HA.
	// Example: "fan" has a window allowing change of fan speed,
	// oscillation, and toggling on/off.
	//
	// Meanwhile, a "switch" is simply a toggle.
	Platform string
	// Declare returns property declarations for a component.
	// These are necessary for creating a [ComponentHandle];
	// see [AttachComponent].
	Properties PropDecls
}

func Canon(cmp Component) string {
	canon := strings.ToLower(rgCanon.ReplaceAllLiteralString(cmp.Alias, "_"))
	return canon
}

// AttachComponent returns a component handle given a
// Component and a [miot.Device], and will fail if
// component cannot be attached even if it is
// non-mandatory.
//
// Callers should skip over non-mandatory components
// returning ErrNoMandatoryProp.
//
// FIXME this function always populates discovery message since
// it needs the same stuff as everything else, but
// it may not be needed.
func AttachComponent(cmp Component, dev *miot.Device, dt DeviceTopic) (ComponentHandle, error) {
	componentTopic := dt.Component(cmp)
	platform := cmp.Platform
	did := dev.DeviceID
	commandTopics := make(TopicMap)
	stateTopics := make(TopicMap)

	decls := cmp.Properties
	cmpDiscov := make(ComponentDiscovery)
	root := componentTopic.AsRoot()

	cmpDiscov["unique_id"] = UniqueID(did, platform, Canon(cmp))
	cmpDiscov["platform"] = platform
	cmpDiscov["name"] = cmp.Alias
	cmpDiscov["~"] = root
	cmpDiscov["availability_topic"] = componentTopic.AvailRel()

	dst := attachPropDeclDst{
		CommandTopics: commandTopics,
		StateTopics:   stateTopics,
		CmpDiscovery:  cmpDiscov,
	}

	for matchName, decl := range decls {
		specProp, ok := dev.PropName(matchName)
		if !ok {
			if decl.Mandatory {
				// property not found. fail if Mandatory.
				return ComponentHandle{}, errors.Join(
					ErrComponentAttach,
					fmt.Errorf("%w: %v", ErrNoMandatoryProp, matchName),
				)
			} else {
				// it's fine, just debug log
				slog.Debug("device has no optional prop", "did", dev.DeviceID, "name", matchName)
				continue
			}
		}

		// try to attach this PropDecl
		err := attachPropDecl(dst, attachPropDeclArgs{
			Decl:           &decl,
			Spec:           &specProp,
			ComponentTopic: &componentTopic,
		})
		if err != nil {
			return ComponentHandle{}, err
		}
	}

	return ComponentHandle{
		cmp:           cmp,
		did:           did,
		platform:      platform,
		CommandTopics: commandTopics,
		StateTopics:   stateTopics,
		Discovery:     cmpDiscov,
		Canon:         Canon(cmp),
		AvailTopic:    componentTopic.AvailTopic(),
	}, nil
}

type attachPropDeclArgs struct {
	Decl           *PropDecl
	Spec           *config.SpecProp
	ComponentTopic *ComponentTopic
}

type attachPropDeclDst struct {
	CommandTopics TopicMap
	StateTopics   TopicMap
	CmpDiscovery  ComponentDiscovery
}

// attachPropDecl uses PropDecl and supporting data to
// populate the following:
//
//   - in-memory command topics
//   - in-memory state topics
//   - prepare component discovery
func attachPropDecl(dst attachPropDeclDst, args attachPropDeclArgs) error {
	attr := args.Decl.Attr()
	decl := args.Decl
	prop := args.Spec
	componentTopic := args.ComponentTopic

	var discovAttrs map[string]any
	var vm wire.ValueMap

	// default to identity map
	vm = &wire.IdentityValueMap{}

	if decl.Expand != nil {
		exp, err := decl.Expand(prop)
		if err != nil {
			return errors.Join(
				ErrComponentAttach,
				err,
			)
		}

		// insert expanded attributes
		discovAttrs = exp.Attributes

		if exp.ValueMap != nil {
			vm = exp.ValueMap
		}
	}
	maps.Insert(dst.CmpDiscovery, maps.All(discovAttrs))

	if prop.Format == wire.MiTypeBool {
		dst.CmpDiscovery["payload_"+attr+"on"] = "true"
		dst.CmpDiscovery["payload_"+attr+"off"] = "false"
	}

	propTopic := componentTopic.Property(decl)
	if prop.Read() {
		// state topic in discov: relative
		dst.CmpDiscovery[attr+"state_topic"] = propTopic.State(false)

		// state topic in table: absolute
		topic := propTopic.State(true)

		dst.StateTopics[topic] = TopicEntry{
			URN:      prop.Urn,
			ValueMap: vm,
		}
	}

	if prop.Write() {
		// command topic in discov: relative
		dst.CmpDiscovery[attr+"command_topic"] = propTopic.Command(false)

		// command topic in table: absolute
		topic := propTopic.Command(true)
		dst.CommandTopics[topic] = TopicEntry{
			URN:      prop.Urn,
			ValueMap: vm,
		}
	}

	return nil
}

// UniqueID calculates unique_id for a component.
func UniqueID(did wire.DeviceID, platform string, canon string) string {
	var sb strings.Builder

	sb.WriteString(BasePath)
	sb.WriteRune('_')
	sb.WriteString(did.String())
	sb.WriteRune('_')
	sb.WriteString(platform)
	sb.WriteRune('_')
	sb.WriteString(canon)

	return sb.String()
}
