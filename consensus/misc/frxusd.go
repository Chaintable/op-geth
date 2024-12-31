package misc

import (
	"github.com/ethereum/go-ethereum/consensus/misc/frxusd"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

func EnsureFrxUSD(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	if !c.IsOptimism() || c.GraniteTime == nil || *c.GraniteTime != timestamp {
		return
	}

	switch c.ChainID.Uint64() {
	case 2521:
		frxusd.RunDevnetMigration(c, timestamp, db)
	case 2522:
		frxusd.RunTestnetMigration(c, timestamp, db)
	default:
		frxusd.RunMainnetMigration(c, timestamp, db)
	}
}
