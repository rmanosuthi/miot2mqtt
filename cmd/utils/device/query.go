package device

import (
	"context"

	"github.com/rmanosuthi/miot2mqtt/cmd/utils/common"
	"github.com/rmanosuthi/miot2mqtt/miot"
)

type devQueryArgs struct {
	Instance common.MinInstance
	Entries  miot.AddDeviceRequests
	Strict   bool
}

// query has the invocation:
//
//	dev -q -P prefix [-v] [-r] -e addr1,token1 -e addr2,token2 -e ...
func query(ctx context.Context, args devQueryArgs) error {
	l := args.Instance.ModeLogger.WithGroup("query")
	for _, req := range args.Entries {
		rd, err := miot.ResolveDevice(ctx, req, l)
		if err != nil {
			if args.Strict {
				return err
			}
			l.Error("resolve device", "reason", err)
			continue
		}
		did := rd.Info.DeviceID
		l := l.WithGroup(did.String())

		l.Info("queried", "result", rd.Info)
	}
	return nil
}
