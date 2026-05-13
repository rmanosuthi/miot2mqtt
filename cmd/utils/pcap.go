package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"github.com/rmanosuthi/miot2mqtt/device"
	"github.com/rmanosuthi/miot2mqtt/wire"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

func replayPcap(mapDevices device.MapDevices, path string, verbose bool, relaxed bool) error {
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
