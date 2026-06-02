package device

import (
	"context"

	"github.com/rmanosuthi/miot2mqtt/cmd/utils/common"
	"github.com/rmanosuthi/miot2mqtt/config"
	"github.com/rmanosuthi/miot2mqtt/miot"
)

type devAddArgs struct {
	Instance   common.MinInstance
	Entries    devEntries
	FetchSpecs bool
}

// add has the invocation:
//
//	dev -a -P prefix [-v] [-d] -e addr1,token1 -e addr2,token2 -e ...
func add(ctx context.Context, args devAddArgs) error {
	l := args.Instance.ModeLogger.WithGroup("add")
	mini := args.Instance

	cfgDevs, err := miot.DevicesToAdd(ctx, miot.AddDeviceArgs{
		Prefix:       mini.PrefixRoot,
		Global:       &mini.Global,
		GlobalLogger: l,
		Reqs:         args.Entries,
	})
	if err != nil {
		return err
	}

	for did, dm := range cfgDevs {
		l := l.WithGroup(did)
		cfgDev := dm.Device
		meta := dm.Meta
		l.Info("new device", "model", dm.Device.Model)

		if args.FetchSpecs {
			var spec config.Spec
			a := config.Args[config.SpecHint]{
				Prefix: mini.PrefixRoot,
				Global: &mini.Global,
				Hint: &config.SpecHint{
					Model:   cfgDev.Model,
					Version: cfgDev.Version,
					Download: &config.SpecDownload{
						URN:     meta.SpecURN,
						Context: ctx,
					},
				},
			}
			err = config.Populate(&spec, a, l)
			if err != nil {
				return err
			}

			l.Info("fetched spec")
		}
	}

	// save
	_, err = miot.LoadDevices(ctx, miot.LoadArgs{
		Prefix:       mini.PrefixRoot,
		Global:       &mini.Global,
		Strict:       true,
		MergeWith:    cfgDevs.Devices(),
		GlobalLogger: l,
	})
	if err != nil {
		return err
	}
	return nil
}
