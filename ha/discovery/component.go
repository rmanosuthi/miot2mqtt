package discovery

import (
	"encoding/json"
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
	// Canonical form of the component.
	canon string
	// HA platform of the component.
	platform string
	// Base topic of the component. Not very useful on its own; see
	// [NewPropTopic].
	Topic         string
	CommandTopics map[PropTopic]config.URN
	StateTopics   map[config.URN]PropTopic
	Discovery     ComponentDiscovery
}

func (h *ComponentHandle) UnmarshalJSON(b []byte) error {
	return fmt.Errorf("ComponentHandle should never be unmarshaled.")
}

func (h *ComponentHandle) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.canon)
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
//	    "value_template":"{{ value_json.temperature}}",
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
//	component name in device discovery's components map: {Canon}
//	unique_id: miot2mqtt_{DeviceID}_{Platform}_{Canon}
//	name: {Alias}
type Component interface {
	// Mandatory tells the resolver this component's initialization must succeed,
	// else the entire device's initialization will be aborted.
	Mandatory() bool
	// Alias is the user-facing name of the component.
	Alias() string
	// Platform is an interaction UI group on HA.
	// Example: "fan" has a window allowing change of fan speed,
	// oscillation, and toggling on/off.
	//
	// Meanwhile, a "switch" is simply a toggle.
	Platform() string
	// Declare returns property declarations for a component.
	// These are necessary for creating a [ComponentHandle];
	// see [AttachComponent].
	Declare() PropDecls
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
func AttachComponent(cmp Component, dev *miot.Device) (ComponentHandle, error) {
	canon := strings.ToLower(rgCanon.ReplaceAllLiteralString(cmp.Alias(), "_"))
	platform := cmp.Platform()
	did := dev.DeviceID
	commandTopics := make(map[PropTopic]config.URN)
	stateTopics := make(map[config.URN]PropTopic)

	var sb strings.Builder
	sb.WriteString(BasePath)
	sb.WriteRune('/')
	sb.WriteString(did.String())
	sb.WriteRune('/')
	sb.WriteString(platform)
	sb.WriteRune('/')
	sb.WriteString(canon)
	cmpTopic := sb.String()
	sb.Reset()

	decls := cmp.Declare()
	cmpDiscov := make(ComponentDiscovery)

	cmpDiscov["unique_id"] = UniqueID(did, platform, canon)
	cmpDiscov["platform"] = platform
	cmpDiscov["name"] = cmp.Alias()
	cmpDiscov["~"] = cmpTopic

	for matchName, decl := range decls {
		prop, ok := dev.PropName(matchName)
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

		// TODO refactor
		// old Resolver.Expand
		attr := decl.Attr()
		topicFrag := decl.TopicFragment()

		var more map[string]any
		var err error
		if decl.More != nil {
			more, err = decl.More(prop)
			if err != nil {
				return ComponentHandle{}, errors.Join(
					ErrComponentAttach,
					err,
				)
			}
		}
		maps.Insert(cmpDiscov, maps.All(more))

		if prop.Format.Type.Name() == "bool" {
			cmpDiscov["payload_"+attr+"on"] = "true"
			cmpDiscov["payload_"+attr+"off"] = "false"
		}

		if prop.Read() {
			// state topic in discov: relative
			sb.WriteString("~/")
			sb.WriteString(topicFrag)
			sb.WriteString("/state")
			cmpDiscov[attr+"state_topic"] = sb.String()
			sb.Reset()

			// state topic in table: absolute
			propTopic := NewPropTopic(cmpTopic, &decl, false)
			stateTopics[prop.Urn] = propTopic
		}

		if prop.Write() {
			// command topic in discov: relative
			sb.WriteString("~/")
			sb.WriteString(topicFrag)
			sb.WriteString("/command")
			cmpDiscov[attr+"command_topic"] = sb.String()
			sb.Reset()

			// command topic in table: absolute
			propTopic := NewPropTopic(cmpTopic, &decl, true)
			commandTopics[propTopic] = prop.Urn
		}
	}

	return ComponentHandle{
		cmp:           cmp,
		did:           did,
		canon:         canon,
		platform:      platform,
		Topic:         cmpTopic,
		CommandTopics: commandTopics,
		StateTopics:   stateTopics,
		Discovery:     cmpDiscov,
	}, nil
}

// Canon returns the canonical form of a component.
func (c *ComponentHandle) Canon() string {
	return c.canon
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
