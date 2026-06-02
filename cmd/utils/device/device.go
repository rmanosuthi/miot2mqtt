// See usage.txt for usage.
package device

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/rmanosuthi/miot2mqtt/cmd/utils/common"
	"github.com/rmanosuthi/miot2mqtt/miot"
)

//go:embed usage.txt
var usageText string

func Entrypoint(ctx context.Context, l *slog.Logger, args []string) error {
	var gf common.GlobalFlags
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, usageText)
	}
	common.Flags(&gf, fs)

	var addDev bool
	var listDev bool
	var queryDev bool

	var relaxed bool
	var download bool
	var entries miot.AddDeviceRequests

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

	if gf.Prefix == "" {
		return common.ErrNoPrefix
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
