package watcher

import (
	"flag"
	"time"

	"github.com/zachfi/zkit/pkg/util"
)

type Config struct {
	NotificationDelay time.Duration
}

func (cfg *Config) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.NotificationDelay, util.PrefixConfig(prefix, "notification-delay"), time.Second, "The time to wait before acting on a change notification.")
}
