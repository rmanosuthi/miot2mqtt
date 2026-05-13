package config

import (
	"os"

	"github.com/BurntSushi/toml"
)

type General struct {
	AllowExternalNetwork bool
}

type MQTT struct {
	Endpoint         string
	Username         string
	Password         string
	KeepAliveSeconds uint16
}

type Global struct {
	General General
	MQTT    MQTT
}

func (cfg *Global) Default(pfx *os.Root, gc *Global, hint *NoHint) error {
	cfg.General = General{
		AllowExternalNetwork: false,
	}
	cfg.MQTT = MQTT{
		Endpoint:         "tcp://127.0.0.1:1883",
		Username:         "username",
		Password:         "password",
		KeepAliveSeconds: 5,
	}
	return nil
}

func (cfg *Global) Suffix(hint *NoHint) (string, error) {
	return "config.toml", nil
}

func (cfg *Global) MarshalFunc() ([]byte, error) {
	return toml.Marshal(cfg)
}

func (cfg *Global) UnmarshalFunc(src []byte) error {
	return toml.Unmarshal(src, cfg)
}
