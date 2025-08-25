package hasher

import (
	"flag"

	"github.com/zachfi/zkit/pkg/util"
)

type Config struct {
	ConcurrencyLimit uint
}

func (cfg *Config) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	f.UintVar(&cfg.ConcurrencyLimit, util.PrefixConfig(prefix, "concurrency"), 2, "The max number of file hashes occuring at one time")
}
