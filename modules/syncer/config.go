package syncer

import (
	"flag"
)

type Config struct {
	// ReportConcurrency uint
}

func (cfg *Config) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	// f.UintVar(&cfg.ReportConcurrency, util.PrefixConfig(prefix, "concurrency"), 100, "The number of route jobs to run at a time")
}
