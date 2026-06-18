package ha

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"strings"

	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/miot/prop"
	"github.com/rmanosuthi/miot2mqtt/wire"
)

var ErrNoMandatoryProp = errors.New("no such mandatory property with name")
var ErrComponentAttach = errors.New("failed to attach component")

// FIXME make more robust
var rgCanon = regexp.MustCompile(`[\s\-]+`)

const BasePath = "miot2mqtt"

// A ComponentHandle is an in-memory representation of
// a [ComponentTemplate] associated with a device.
type ComponentHandle struct {
	// A reference to the template is kept just in case.
	// FIXME remove?
	cmp ComponentTemplate
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

// A ComponentTemplate is template of a controllable single-platform entity
// belonging to a [Device].
//
// Example: a fan may have several Components:
//
//   - Platform Fan: on/off, fan speed, horizontal oscillation
//   - Platform Number: horizontal swing angle
//   - Platform Number: vertical swing angle
//   - Platform Switch: vertical oscillation
//
// [ComponentDiscovery] forms the discovery message.
// User interaction is done through [ComponentHandle].
//
// A component is simply marked as online whenever Device starts and
// offline when the program exits.
// This will most likely change in the future.
type ComponentTemplate struct {
	// Mandatory tells the resolver this component's initialization must succeed,
	// else the entire device's initialization will be aborted.
	Mandatory bool
	// Alias is the user-facing name of the component.
	// This shows up on Home Assistant's UI as the label.
	Alias string
	// Service is the spec service property declarations should be matched from.
	//
	// Example: "physical-controls-locked" will only allow properties
	// under the service
	// "urn:miot-spec-v2:service:physical-controls-locked:..."
	// to be matched.
	Service string
	// Platform is an interaction UI group on HA.
	//
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

// Canon returns a canonical form of a component
// for use as
// the key in the discovery message's components map and
// for deriving [UniqueID].
//
// See [ComponentDiscovery] for an example.
func (cmp *ComponentTemplate) Canon() string {
	canon := strings.ToLower(rgCanon.ReplaceAllLiteralString(cmp.Alias, "_"))
	return canon
}

// AttachComponent returns a component handle given a
// ComponentTemplate and a [miot.Device].
//
// Errors encountered parsing non-mandatory properties
// are always ignored.
//
// FIXME this function always populates discovery message since
// it needs the same stuff as everything else, but
// it may not be needed.
func AttachComponent(cmp ComponentTemplate, dev *miot.Device, dt DeviceTopic) (ComponentHandle, error) {
	componentTopic := dt.Component(cmp)
	platform := cmp.Platform
	did := dev.DeviceID
	commandTopics := make(TopicMap)
	stateTopics := make(TopicMap)

	decls := cmp.Properties
	cmpDiscov := make(ComponentDiscovery)
	root := componentTopic.AsRoot()
	canon := cmp.Canon()

	cmpDiscov["unique_id"] = UniqueID(did, platform, canon)
	cmpDiscov["platform"] = platform
	cmpDiscov["name"] = cmp.Alias
	cmpDiscov["~"] = root
	cmpDiscov["availability_topic"] = componentTopic.AvailRel()

	dst := attachPropDeclDst{
		CommandTopics: commandTopics,
		StateTopics:   stateTopics,
		CmpDiscovery:  cmpDiscov,
	}

	svc, ok := dev.ServiceName(cmp.Service)
	if !ok {
		return ComponentHandle{}, errors.Join(
			ErrComponentAttach,
			fmt.Errorf("no service with name %v", cmp.Service),
		)
	}

	for matchName, decl := range decls {
		pair, ok := dev.FindPropKey(&svc, matchName)
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
			Spec:           &pair.Spec,
			Key:            pair.Key,
			ComponentTopic: &componentTopic,
		})
		if err != nil {
			if decl.Mandatory {
				return ComponentHandle{}, err
			} else {
				slog.Warn("attach property", "reason", err)
				continue
			}
		}
	}

	return ComponentHandle{
		cmp:           cmp,
		did:           did,
		platform:      platform,
		CommandTopics: commandTopics,
		StateTopics:   stateTopics,
		Discovery:     cmpDiscov,
		Canon:         canon,
		AvailTopic:    componentTopic.AvailTopic(),
	}, nil
}

type attachPropDeclArgs struct {
	Decl           *PropDecl
	Spec           *config.SpecProp
	Key            prop.PropKey
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
			PropKey:  args.Key,
			ValueMap: vm,
		}
	}

	if prop.Write() {
		// command topic in discov: relative
		dst.CmpDiscovery[attr+"command_topic"] = propTopic.Command(false)

		// command topic in table: absolute
		topic := propTopic.Command(true)
		dst.CommandTopics[topic] = TopicEntry{
			PropKey:  args.Key,
			ValueMap: vm,
		}
	}

	return nil
}
