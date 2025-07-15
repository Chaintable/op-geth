package misc

import (
	"github.com/ethereum/go-ethereum/consensus/misc/fraxisthmus"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

func EnsureFraxIsthmusMigration(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	if !c.IsOptimism() || c.IsthmusTime == nil || *c.IsthmusTime != timestamp {
		return
	}

	fraxisthmus.RunMigration(c, timestamp, db)
}
