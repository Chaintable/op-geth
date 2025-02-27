package misc

import (
	"github.com/ethereum/go-ethereum/consensus/misc/fraxtokensproxies"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

func EnsureFraxTokensProxies(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	if !c.IsOptimism() || c.HoloceneTime == nil || *c.HoloceneTime != timestamp {
		return
	}

	fraxtokensproxies.RunMigration(c, timestamp, db)
}
