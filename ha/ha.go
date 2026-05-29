package ha

import (
	"log/slog"

	paho "github.com/eclipse/paho.golang/paho"
	"github.com/rmanosuthi/miot2mqtt/config"
)

type Handle struct {
	global *config.Global
	mqtt   paho.Client
	l      *slog.Logger
}
