package device

import (
	"context"

	"github.com/rmanosuthi/miot2mqtt/cmd/utils/common"
	"github.com/rmanosuthi/miot2mqtt/miot"
)

type devListArgs struct {
	Instance common.FullInstance
	// TODO filter from this
	Entries miot.AddDeviceRequests
}

// list has the invocation:
//
//	dev -l -P prefix [-v]
func list(ctx context.Context, args devListArgs) error {
	l := args.Instance.ModeLogger.WithGroup("list")
	for did, device := range args.Instance.Devices {
		l := l.WithGroup(did.String())
		l.Info("device", "info", device)
		for actionName, action := range device.Actions {
			l.Info("action", "name", actionName, "spec", action)
		}
		for propName, prop := range device.Props {
			l.Info("prop", "name", propName, "spec", prop)
		}
	}

	return nil
}
