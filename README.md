# miot2mqtt

## Prefix

`miot2mqtt` needs a dedicated folder to store its state and config.
This is shared between all binaries in `cmd/` for simplicity.
The prefix must be given using `-P {prefix}`.

Example: `/var/lib/miot2mqtt/`

`miot2mqtt` assumes exclusive access to the prefix; don't edit anything while in operation.

```
/config.toml
/cache/devices.toml
/vendor/miot_instances.json
/vendor/spec/
```
