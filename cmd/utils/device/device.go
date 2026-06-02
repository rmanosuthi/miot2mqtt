/*
Device is a mode of utils for device related functions.

Documentation for
-P prefix and -v
has been omitted for brevity.

# Adding devices

This usage is for adding new devices to the prefix:

	utils dev -a -P prefix [-v] [-d] -e addr1,token1 -e addr2,token2 -e ...

-a is mutually exclusive with -l and -q.

Flags:

	-d
		Download devices' spec files if needed.
		Requires AllowExternalNetwork in config.toml.

	-e addr,token
		Specify a device entry using an IP address
		and a hex-encoded token without a 0x prefix.
		The two must be separated by a comma.
		This flag may be repeated for each device to be added.

# Listing devices

This usage is for listing the prefix's devices:

	utils dev -l -P prefix [-v]

-l is mutually exclusive with -a and -q.
The program will not attempt to contact the enumerated devices.

# Querying devices

This usage is for querying devices:

	utils dev -q -P prefix [-v] [-r] -e addr1,token1 -e addr2,token2 -e ...

-q is mutually exclusive with -a and -l.
The devices do not have to be
registered in the prefix.

Flags:

	-e addr,token
		Specify a device entry using an IP address
		and a hex-encoded token without a 0x prefix.
		The two must be separated by a comma.
		This flag may be repeated for each device to be queried.

	-r
		Relaxed mode. Do not quit the program as soon as
		an error is encountered querying a device.
*/
package device

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"strings"

	"github.com/rmanosuthi/miot2mqtt/cmd/utils/common"
	"github.com/rmanosuthi/miot2mqtt/miot"
)

type devEntries []miot.AddDeviceRequest

func (de *devEntries) String() string {
	return ""
}

func (de *devEntries) Set(value string) error {
	v := strings.TrimSpace(value)
	subs := strings.Split(v, ",")
	if subs[0] == "" {
		return fmt.Errorf("no address")
	}
	if subs[1] == "" {
		return fmt.Errorf("no token")
	}

	*de = append(*de, miot.AddDeviceRequest{
		IPAddr: subs[0],
		Token:  subs[1],
	})
	return nil
}

func Entrypoint(ctx context.Context, l *slog.Logger, args []string) error {
	var gf common.GlobalFlags
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	common.Flags(&gf, fs)

	var addDev bool
	var listDev bool
	var queryDev bool

	var relaxed bool
	var download bool
	var entries devEntries

	// modes
	fs.BoolVar(&addDev, "a", false, "add devices")
	fs.BoolVar(&listDev, "l", false, "list devices")
	fs.BoolVar(&queryDev, "q", false, "query devices")

	fs.BoolVar(&relaxed, "r", false, "don't quit on failure")
	fs.BoolVar(&download, "d", false, "download device specs")
	fs.Var(&entries, "e", "new device entry, repeatable, format addr,token")

	err := fs.Parse(args)
	if err != nil {
		fs.Usage()
		return err
	}

	if !addDev && !listDev && !queryDev {
		fs.Usage()
		return nil
	}

	minInst, err := common.MinInit(ctx, l, &gf)
	if err != nil {
		return err
	}

	if addDev {
		if len(entries) == 0 {
			l.Error("at least one device entry must be given")
			return nil
		}
		// add
		return add(ctx, devAddArgs{
			Instance:   minInst,
			Entries:    entries,
			FetchSpecs: download,
		})
	} else if listDev {
		// list
		fullInst, err := common.FullInit(ctx, minInst)
		if err != nil {
			return err
		}

		return list(ctx, devListArgs{
			Instance: fullInst,
			Entries:  entries,
		})
	} else {
		if len(entries) == 0 {
			l.Error("at least one device entry must be given")
			return nil
		}
		// query
		return query(ctx, devQueryArgs{
			Instance: minInst,
			Entries:  entries,
			Strict:   !relaxed,
		})
	}
}
