package config

import "errors"

var ErrPopulate = errors.New("populate config")
var ErrLoad = errors.New("load config")
var ErrFlush = errors.New("flush config")
var ErrNoExtNet = errors.New("external network access disabled")
var ErrNoSpecHint = errors.New("tried to fetch spec with no hint")
var ErrUnmarshalUrn = errors.New("unmarshal urn")
