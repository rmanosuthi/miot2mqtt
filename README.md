# miot2mqtt

(Unofficial) Bridge Xiaomi Miot devices to Home Assistant using MQTT, just like zigbee2mqtt.

Currently, only fans are supported. This project is still in its infancy.

## Getting Started

You'll need a Go compiler, your devices' IPs and tokens.
There are programs out there for fetching tokens. [Here's one.](https://github.com/PiotrMachowski/Xiaomi-cloud-tokens-extractor)

1. `go build ./cmd/daemon`
2. Copy the resulting `./daemon` somewhere.
3. Create a prefix folder, see section below.
4. Run `daemon -P {prefix}`.
On first run it will create a default config in `{prefix}/config.toml` and exit.
5. Edit the created config.
6. You'll want to add some devices. Run `daemon -P {prefix} -e "{ipaddr},{tokenhex}"`.

Example: `daemon -P ~/m2m/ -e "192.168.1.154,a2b9d813107c8b2ece0bd517ddf860bc" -e "192.168.1.75,39422d3039323731364544383934320a"`

## Prefix

`miot2mqtt` needs a dedicated folder to store its state and config.
This is shared between all binaries in `cmd/` for simplicity.
The prefix must be given using `-P {prefix}`.

Example: `/var/lib/miot2mqtt/`

`miot2mqtt` assumes exclusive access to the prefix; don't edit anything while in operation.

Do not edit anything in `/cache/` as the program is not tolerant of any errors.

The prefix will generally have this structure:

```
/config.toml
/cache/devices.toml
/vendor/miot_instances.json
/vendor/spec/
```

## Vendor files

Devices need "spec" files and an "instances" file to operate.
These can be fetched automatically when `config.toml` has `AllowExternalNetwork = true`.

Otherwise you must provide the files yourself in `vendor/`. But really, just let me handle it.

## How's this different from *that popular project?*

- It's an alternative.
- Certainly not production-ready.
- Exposes more specific options in HA. Supports much less devices though.
- Less coupled to HA; can run on an entirely different device.
- Written in Go and uses a fair amount of goroutines, so should be more responsive.
- Way less emojis.

## Extent of AI assistance?

99% handcrafted; no AI assistance except for Google's unavoidable RAG responses when searching.
I deliberately want to work on this project myself.

See [securebin](https://github.com/rmanosuthi/securebin) for a heavily AI-assisted project.
