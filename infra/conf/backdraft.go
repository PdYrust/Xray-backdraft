package conf

import (
	"github.com/xtls/xray-core/app/backdraft"
	"github.com/xtls/xray-core/common/errors"
)

type BackdraftConfig struct {
	Target      string `json:"target"`
	Destination string `json:"destination"`
	Path        string `json:"path"`
	ObjectSize  string `json:"objectSize"`
	CloseWindow string `json:"closeWindow"`
	Abort       string `json:"abort"`
	ReadBytes   int64  `json:"readBytes"`
	Rate        int64  `json:"rate"`
	Jitter      int64  `json:"jitter"`
	Concurrency int64  `json:"concurrency"`
	Warmup      string `json:"warmup"`
	Outbound    string `json:"outbound"`
}

// Build implements Buildable.
func (c *BackdraftConfig) Build() (*backdraft.Config, error) {
	if c == nil {
		return nil, errors.New("nil configuration")
	}
	if c.Target == "" {
		return nil, errors.New("target is required")
	}

	config := &backdraft.Config{
		Target:      c.Target,
		Destination: c.Destination,
		Path:        c.Path,
		ObjectSize:  c.ObjectSize,
		CloseWindow: c.CloseWindow,
		Abort:       c.Abort,
		ReadBytes:   c.ReadBytes,
		Rate:        c.Rate,
		Jitter:      c.Jitter,
		Concurrency: c.Concurrency,
		Warmup:      c.Warmup,
		Outbound:    c.Outbound,
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}
