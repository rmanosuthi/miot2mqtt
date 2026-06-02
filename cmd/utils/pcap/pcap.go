// See usage.txt for usage.
package pcap

import (
	"bytes"
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/rmanosuthi/miot2mqtt/cmd/utils/common"
	"github.com/rmanosuthi/miot2mqtt/miot"
	"github.com/rmanosuthi/miot2mqtt/wire"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

//go:embed usage.txt
var usageText string

func Entrypoint(ctx context.Context, l *slog.Logger, args []string) error {
	var gf common.GlobalFlags
	fs := flag.NewFlagSet("pcap", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, usageText)
	}
	common.Flags(&gf, fs)

	var pcapPath string
	var relaxed bool

	fs.StringVar(&pcapPath, "f", "", "pcap file path")
	fs.BoolVar(&relaxed, "r", false, "relaxed mode")

	err := fs.Parse(args)
	if err != nil {
		return err
	}

	if gf.Prefix == "" {
		return common.ErrNoPrefix
	}

	minInst, err := common.MinInit(ctx, l, &gf)
	if err != nil {
		return err
	}

	fullInst, err := common.FullInit(ctx, minInst)
	if err != nil {
		return err
	}

	return replayPcap(fullInst.Devices, pcapPath, gf.Verbose, relaxed)
}

func replayPcap(mapDevices miot.MapDevices, path string, verbose bool, relaxed bool) error {
	var direction rune
	handle, err := pcap.OpenOffline(path)
	if err != nil {
		return err
	}

	src := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range src.Packets() {
		udpL := packet.Layer(layers.LayerTypeUDP)
		if udpL == nil {
			continue
		}
		udp, _ := udpL.(*layers.UDP)
		if !(udp.SrcPort == 54321 || udp.DstPort == 54321) {
			continue
		}
		rawPkt := udp.Payload
		if bytes.Equal(rawPkt, wire.PingPacket) {
			fmt.Printf("ping\n")
			continue
		}

		did, err := wire.GetDeviceID(rawPkt)
		if err != nil {
			slog.Warn("failed to get did", "reason", err)
			continue
		}
		dev, ok := mapDevices[did]
		if !ok {
			slog.Warn("token not found", "did", did)
			continue
		}
		token := dev.Token
		pkt, err := token.Unmarshal(rawPkt)
		if err != nil {
			if relaxed {
				slog.Warn("failed to parse packet", "reason", err)
				continue
			} else {
				return err
			}
		}
		if udp.SrcPort == 54321 {
			// response
			direction = '<'
		} else {
			// send
			direction = '>'
		}
		payload := pkt.Payload
		fmt.Printf("[%v] %c %v: %s\n", pkt.Timestamp, direction, pkt.DeviceID, payload)
	}
	return nil
}
