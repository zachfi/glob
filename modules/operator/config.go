package operator

import (
	"flag"

	"github.com/google/uuid"
	"github.com/zachfi/zkit/pkg/util"
)

type Config struct {
	SyncConcurrency uint

	SyncTargets []SyncTarget `yaml:"sync_targets,omitempty"`
	SyncPeers   []SyncPeer   `yaml:"sync_peers,omitempty"`
}

type SyncTarget struct {
	ID     uuid.UUID `yaml:"id"`
	Source string    `yaml:"source"`
}

type SyncPeer struct {
	Addr    string      `yaml:"addr"`
	Targets []uuid.UUID `yaml:"targets"`
}

func (cfg *Config) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	f.UintVar(&cfg.SyncConcurrency, util.PrefixConfig(prefix, "sync-concurrency"), 1, "The number of sync operations that can be performed concurrently")
}
